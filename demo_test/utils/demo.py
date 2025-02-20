import threading
import time

def de():
    time.sleep(5)
    t3 = threading.currentThread()
    print('#' * 30)
    print('Thread id : %d ' % t3.ident)
    print('Thread name : %s' % t3.getName())
    print('#' * 30)
    for i in range(100):
        print(i)