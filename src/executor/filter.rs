use boolinator::Boolinator;
use nom_sql::{
    Column, ConditionBase, ConditionExpression, ConditionTree, Literal, Operator, SelectStatement,
    Table,
};
use std::fmt::Debug;
use thiserror::Error;

use crate::data::{Row, Value};
use crate::executor::{fetch_select_params, select, BlendContext, FilterContext};
use crate::result::Result;
use crate::storage::Store;

#[derive(Error, Debug, PartialEq)]
pub enum FilterError {
    #[error("nested select row not found")]
    NestedSelectRowNotFound,

    #[error("UnreachableConditionBase")]
    UnreachableConditionBase,

    #[error("unimplemented")]
    Unimplemented,
}

pub struct Filter<'a, T: 'static + Clone + Debug> {
    storage: &'a dyn Store<T>,
    where_clause: Option<&'a ConditionExpression>,
    context: Option<&'a FilterContext<'a>>,
}

impl<'a, T: 'static + Clone + Debug> Filter<'a, T> {
    pub fn new(
        storage: &'a dyn Store<T>,
        where_clause: Option<&'a ConditionExpression>,
        context: Option<&'a FilterContext<'a>>,
    ) -> Self {
        Self {
            storage,
            where_clause,
            context,
        }
    }

    pub fn check(&self, table: &Table, columns: &Vec<Column>, row: &Row) -> Result<bool> {
        let context = FilterContext::new(table, columns, row, self.context);

        match self.where_clause {
            Some(expr) => check_expr(self.storage, &context, expr),
            None => Ok(true),
        }
    }

    pub fn check_blended(&self, blend_context: &BlendContext<'_, T>) -> Result<bool> {
        match self.where_clause {
            Some(expr) => check_blended_expr(self.storage, self.context, blend_context, expr),
            None => Ok(true),
        }
    }
}

pub struct BlendedFilter<'a, T: 'static + Clone + Debug> {
    filter: &'a Filter<'a, T>,
    context: Option<&'a BlendContext<'a, T>>,
}

impl<'a, T: 'static + Clone + Debug> BlendedFilter<'a, T> {
    pub fn new(filter: &'a Filter<'a, T>, context: Option<&'a BlendContext<'a, T>>) -> Self {
        Self { filter, context }
    }

    pub fn check(&self, table: &Table, columns: &Vec<Column>, row: &Row) -> Result<bool> {
        let BlendedFilter {
            filter:
                Filter {
                    storage,
                    where_clause,
                    context: next,
                },
            context: blend_context,
        } = self;

        let filter_context = FilterContext::new(table, columns, row, *next);

        where_clause.map_or(Ok(true), |expr| match blend_context {
            Some(blend_context) => {
                check_blended_expr(*storage, Some(&filter_context), blend_context, expr)
            }
            None => check_expr(*storage, &filter_context, expr),
        })
    }
}

enum Parsed<'a> {
    LiteralRef(&'a Literal),
    ValueRef(&'a Value),
    Value(Value),
}

enum ParsedList<'a, T: 'static + Debug> {
    LiteralRef(&'a Vec<Literal>),
    Value {
        storage: &'a dyn Store<T>,
        statement: &'a SelectStatement,
        filter_context: &'a FilterContext<'a>,
    },
    Parsed(Parsed<'a>),
}

impl<'a> PartialEq for Parsed<'a> {
    fn eq(&self, other: &Parsed<'a>) -> bool {
        use Parsed::*;

        match (self, other) {
            (LiteralRef(lr), LiteralRef(lr2)) => lr == lr2,
            (LiteralRef(lr), ValueRef(vr)) => vr == lr,
            (LiteralRef(lr), Value(v)) => &v == lr,
            (Value(v), LiteralRef(lr)) => &v == lr,
            (Value(v), ValueRef(vr)) => &v == vr,
            (Value(v), Value(v2)) => v == v2,
            (ValueRef(vr), LiteralRef(lr)) => vr == lr,
            (ValueRef(vr), ValueRef(vr2)) => vr == vr2,
            (ValueRef(vr), Value(v)) => &v == vr,
        }
    }
}

impl Parsed<'_> {
    fn exists_in<T: 'static + Clone + Debug>(&self, list: ParsedList<'_, T>) -> Result<bool> {
        Ok(match list {
            ParsedList::Parsed(parsed) => &parsed == self,
            ParsedList::LiteralRef(literals) => literals
                .iter()
                .any(|literal| &Parsed::LiteralRef(&literal) == self),
            ParsedList::Value {
                storage,
                statement,
                filter_context,
            } => {
                let params = fetch_select_params(storage, statement)?;
                let v = select(storage, statement, &params, Some(filter_context))?
                    .map(|row| row?.take_first_value())
                    .filter_map(|value| {
                        value.map_or_else(
                            |error| Some(Err(error)),
                            |value| (&Parsed::Value(value) == self).as_some(Ok(())),
                        )
                    })
                    .next()
                    .transpose()?
                    .is_some();

                v
            }
        })
    }
}

fn parse_expr<'a, T: 'static + Clone + Debug>(
    storage: &'a dyn Store<T>,
    filter_context: &'a FilterContext<'a>,
    expr: &'a ConditionExpression,
) -> Result<Parsed<'a>> {
    let parse_base = |base: &'a ConditionBase| match base {
        ConditionBase::Field(column) => filter_context
            .get_value(&column)
            .map(|value| Parsed::ValueRef(value)),
        ConditionBase::Literal(literal) => Ok(Parsed::LiteralRef(literal)),
        ConditionBase::NestedSelect(statement) => {
            let params = fetch_select_params(storage, statement)?;
            let value = select(storage, statement, &params, Some(filter_context))?
                .map(|row| row?.take_first_value())
                .next()
                .ok_or(FilterError::NestedSelectRowNotFound)??;

            Ok(Parsed::Value(value))
        }
        ConditionBase::LiteralList(_) => Err(FilterError::UnreachableConditionBase.into()),
    };

    match expr {
        ConditionExpression::Base(base) => parse_base(&base),
        _ => Err(FilterError::Unimplemented.into()),
    }
}

fn parse_in_expr<'a, T: 'static + Clone + Debug>(
    storage: &'a dyn Store<T>,
    filter_context: &'a FilterContext<'a>,
    expr: &'a ConditionExpression,
) -> Result<ParsedList<'a, T>> {
    let parse_base = |base: &'a ConditionBase| match base {
        ConditionBase::Field(column) => filter_context
            .get_value(&column)
            .map(|value| ParsedList::Parsed(Parsed::ValueRef(value))),
        ConditionBase::Literal(literal) => Ok(ParsedList::Parsed(Parsed::LiteralRef(literal))),
        ConditionBase::LiteralList(literals) => Ok(ParsedList::LiteralRef(literals)),
        ConditionBase::NestedSelect(statement) => Ok(ParsedList::Value {
            storage,
            statement,
            filter_context,
        }),
    };

    match expr {
        ConditionExpression::Base(base) => parse_base(&base),
        _ => Err(FilterError::Unimplemented.into()),
    }
}

fn check_expr<'a, T: 'static + Clone + Debug>(
    storage: &'a dyn Store<T>,
    filter_context: &'a FilterContext<'a>,
    expr: &'a ConditionExpression,
) -> Result<bool> {
    let check = |expr| check_expr(storage, filter_context, expr);
    let parse = |expr| parse_expr(storage, filter_context, expr);
    let parse_in = |expr| parse_in_expr(storage, filter_context, expr);

    let check_tree = |tree: &'a ConditionTree| {
        let zip_check = || Ok((check(&tree.left)?, check(&tree.right)?));
        let zip_parse = || Ok((parse(&tree.left)?, parse(&tree.right)?));
        let zip_in = || Ok((parse(&tree.left)?, parse_in(&tree.right)?));

        match tree.operator {
            Operator::Equal => zip_parse().map(|(l, r)| l == r),
            Operator::NotEqual => zip_parse().map(|(l, r)| l != r),
            Operator::And => zip_check().map(|(l, r)| l && r),
            Operator::Or => zip_check().map(|(l, r)| l || r),
            Operator::In => zip_in().and_then(|(l, r)| l.exists_in(r)),
            _ => Err(FilterError::Unimplemented.into()),
        }
    };

    match expr {
        ConditionExpression::ComparisonOp(tree) => check_tree(&tree),
        ConditionExpression::LogicalOp(tree) => check_tree(&tree),
        ConditionExpression::NegationOp(expr) => check(expr).map(|b| !b),
        ConditionExpression::Bracketed(expr) => check(expr),
        ConditionExpression::Arithmetic(_) | ConditionExpression::Base(_) => {
            Err(FilterError::Unimplemented.into())
        }
    }
}

fn check_blended_expr<T: 'static + Clone + Debug>(
    storage: &dyn Store<T>,
    filter_context: Option<&FilterContext<'_>>,
    blend_context: &BlendContext<'_, T>,
    expr: &ConditionExpression,
) -> Result<bool> {
    let BlendContext {
        table,
        columns,
        row,
        next,
        ..
    } = blend_context;

    let filter_context = FilterContext::new(table, &columns, &row, filter_context);

    match next {
        Some(blend_context) => {
            check_blended_expr(storage, Some(&filter_context), blend_context, expr)
        }
        None => check_expr(storage, &filter_context, expr),
    }
}
