import subprocess
import threading
import queue
import os

# 配置
command = ["go", "test", "-run", "3B"]  # 要执行的命令
output_file = "3B.log"        # 错误日志文件
concurrency = 10                        # 并发数
total_tests = 100                       # 测试总次数

# 任务队列
task_queue = queue.Queue()
for i in range(total_tests):
    task_queue.put(i)

# 锁
lock = threading.Lock()
terminate_flag = threading.Event()  # 用于标志是否停止所有线程

def worker():
    while not task_queue.empty() and not terminate_flag.is_set():
        test_id = task_queue.get()
        try:
            # 执行命令
            result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            if result.returncode != 0:
                # 如果返回值非零，保存输出并终止
                with lock:
                    with open(output_file, "w") as f:
                        f.write(f"Test ID: {test_id}\n")
                        f.write(result.stdout)
                        f.write(result.stderr)
                    terminate_flag.set()  # 设置终止标志
                    print(f"Test {test_id} failed. Output saved to {output_file}.")
            else:
                print(f"Test {test_id} successfully")
        except Exception as e:
            with lock:
                print(f"Error while running test {test_id}: {e}")
        finally:
            task_queue.task_done()

# 创建线程池
threads = []
for _ in range(concurrency):
    thread = threading.Thread(target=worker)
    thread.start()
    threads.append(thread)

# 等待所有线程完成
for thread in threads:
    thread.join()

if not terminate_flag.is_set():
    print("All tests completed successfully!")


