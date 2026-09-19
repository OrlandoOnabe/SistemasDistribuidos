import zmq

context = zmq.Context()


sub = context.socket(zmq.XSUB)
sub.bind("tcp://*:5555")

pub = context.socket(zmq.XPUB)
pub.bind("tcp://*:5556")

zmq.proxy(sub, pub)

sub.close()
pub.close()
context.close()