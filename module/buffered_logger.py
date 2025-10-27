import logging
import threading
import time
from queue import Queue, Empty

class BufferedLogger:
    
    def __init__(self, log_file, buffer_size=1000, flush_interval=5.0):
        self.log_file = log_file                    # 日志文件路径
        self.buffer_size = buffer_size              # 缓冲区大小阈值
        self.flush_interval = flush_interval        # 刷新时间间隔
        self.buffer = []                            # 本地缓冲区（保留兼容性）
        self.queue = Queue()                        # 线程安全的消息队列
        self.stop_event = threading.Event()        # 线程停止事件
        self.lock = threading.Lock()               # 线程锁（保留兼容性）
        
        # 启动后台写入线程
        # daemon=True 表示守护线程，主程序结束时自动结束
        self.writer_thread = threading.Thread(target=self._writer_worker, daemon=True)
        self.writer_thread.start()
    
    def info(self, message):
        self.queue.put(message)
    
    def _writer_worker(self):
        buffer = []                                 # 本地消息缓冲区
        last_flush = time.time()                   # 上次刷新时间戳
        
        # 打开日志文件，使用UTF-8编码
        with open(self.log_file, 'w', encoding='utf-8') as f:
            # 主循环：持续处理消息直到收到停止信号
            while not self.stop_event.is_set():
                try:
                    # 从队列获取消息，超时时间1秒
                    # 超时设计的目的：定期检查停止信号和待刷新数据
                    message = self.queue.get(timeout=1.0)
                    buffer.append(message)
                    
                    # 检查是否需要刷新缓冲区
                    now = time.time()
                    if (len(buffer) >= self.buffer_size or           # 缓冲区满了
                        now - last_flush >= self.flush_interval):    # 超过时间间隔
                        
                        # 批量写入所有缓冲的消息
                        for msg in buffer:
                            f.write(msg + '\n')
                        f.flush()                                    # 强制刷新到磁盘
                        buffer.clear()                               # 清空缓冲区
                        last_flush = now                             # 更新刷新时间
                        
                except Empty:
                    # 队列超时（1秒内没有新消息）
                    # 这是正常情况，用于定期检查和维护
                    if buffer:
                        # 如果有待写入的数据，立即写入
                        for msg in buffer:
                            f.write(msg + '\n')
                        f.flush()
                        buffer.clear()
                        last_flush = time.time()
            
            # 程序结束处理：确保所有消息都被写入
            # 这部分代码在收到停止信号后执行
            while not self.queue.empty():
                try:
                    # 非阻塞方式获取剩余消息
                    message = self.queue.get_nowait()
                    buffer.append(message)
                except Empty:
                    # 队列已空，跳出循环
                    break
            
            # 写入最后的缓冲数据
            if buffer:
                for msg in buffer:
                    f.write(msg + '\n')
                f.flush()
    
    def close(self):
        # 设置停止事件，通知后台线程停止工作
        self.stop_event.set()
        
        # 等待后台线程完成工作
        if self.writer_thread.is_alive():
            # 最多等待5秒，避免程序无限等待
            self.writer_thread.join(timeout=5.0)
            
            # 如果线程仍然活着，说明可能有问题
            # 在生产环境中可以添加警告日志
