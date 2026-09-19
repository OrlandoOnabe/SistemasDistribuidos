import zmq
import random
from time import sleep

context = zmq.Context()
pub = context.socket(zmq.PUB)
pub.connect("tcp://127.0.0.1:5555")

while True:
    message = f"dado {random.randint(1, 6)}"
    print(f"message: {message}", flush=True)
    pub.send_string(message)
    sleep(1)

pub.close()
context.close()
