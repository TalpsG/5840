use std::{fs::File, process::Command, thread};

fn main() {
    let num_threads = 100; // 启动的线程数量
    let mut handles = vec![];
    let mut results = vec![];

    for i in 0..num_threads {
        let handle = thread::spawn(move || {
            let out_file = File::create(format!("{}.log", i)).expect("open file error");
            let cmd = "go test -run 2A --race";
            let output = Command::new("bash")
                .arg("-c")
                .arg(cmd)
                .stdout(out_file)
                .status()
                .expect("cmd execute fail");
            output
        });
        handles.push(handle);
    }

    // 收集所有线程的返回值
    for handle in handles {
        let result = handle.join().unwrap();
        results.push(result);
    }

    // 打印所有结果
    for (_, result) in results.iter().enumerate() {
        if !result.success() {
            println!("{}", result.code().expect("errorcode fail"))
        }
    }
}
