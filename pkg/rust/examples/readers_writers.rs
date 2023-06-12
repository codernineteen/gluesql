#[cfg(feature = "sled-storage")]
mod reader_writer {
    use {
        futures::executor::block_on,
        gluesql::{
            prelude::Glue, 
            sled_storage::SledStorage,
        },
        std::sync::{Arc, RwLock}
    };

    pub async fn run() {
        let storage = SledStorage::new("/tmp/gluesql/read_write_lock").expect("Something went wrong!");
        let mut glue = Glue::new(storage.clone());
        let queries = "
            CREATE TABLE IF NOT EXISTS enrollment (student_id INTEGER);
            DELETE FROM enrollment;
        ";

        glue.execute(queries).await.unwrap();
        // Create a shared data structure
        let data = Arc::new(RwLock::new(0));
        
        for i in 0..200 {
            let data_clone = Arc::clone(&data);
            let insert_storage = storage.clone();

            std::thread::spawn(move || {
                if i % 2 == 0 {
                    // Read lock
                    let mut retry_count = 0;
                    let mut glue = Glue::new(insert_storage);
                    loop {
                        match data_clone.read() {
                            Ok(read_lock) => {
                                let query = format!("INSERT INTO enrollment (student_id) VALUES ({})", *read_lock);
        
                                if let Err(err) = block_on(glue.execute(query.as_str())) {
                                    println!("Error executing query: {}", err);
                                    retry_count += 1;
        
                                    if retry_count >= 3 {
                                        println!("Max retry count reached. Exiting.");
                                        break;
                                    }
        
                                    println!("Retrying after 1 second...");
                                    std::thread::sleep(std::time::Duration::from_secs(1));
                                    continue;
                                }
        
                                break;
                            }
                            Err(_) => {
                                println!("Error acquiring read lock. Retrying after 1 second...");
                                std::thread::sleep(std::time::Duration::from_secs(1));
                                continue;
                            }
                        }
                    }
    
                } else {
                    // Write lock
                    let mut write_lock = data_clone.write().unwrap();
                    *write_lock += 1;
                    println!("Thread {} wrote: {}", i, *write_lock);
                }
            });
        }


        std::thread::sleep(std::time::Duration::from_secs(2));

        let select_query = "
            SELECT * FROM enrollment
        ";
        let payloads = glue.execute(select_query).await.unwrap();
        for payload in payloads {
            println!("{:?}", payload);
        }
        println!("Enrollment process successfully ended");
    }
}

fn main() {
    #[cfg(feature = "sled-storage")]
    futures::executor::block_on(reader_writer::run());
}