# AMQP 1.0 Flow Control Notes

## [Spec](http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-transport-v1.0-os.html#doc-flow-control)

### delivery-count

The delivery-count is initialized by the sender when a link endpoint is created, and is incremented whenever a message is sent. Only the sender MAY independently modify this field. The receiver's value is calculated based on the last known value from the sender and any subsequent messages received on the link. Note that, despite its name, the delivery-count is not a count but a sequence number initialized at an arbitrary point by the sender.

### link-credit

The link-credit variable defines the current maximum legal amount that the delivery-count can be increased by. This identifies a delivery-limit that can be computed by adding the link-credit to the delivery-count.

Only the receiver can independently choose a value for this field. The sender's value MUST always be maintained in such a way as to match the delivery-limit identified by the receiver. This means that the sender's link-credit variable MUST be set according to this formula when flow information is given by the receiver:

link-credit(snd) := delivery-count(rcv) + link-credit(rcv) - delivery-count(snd).

In the event that the receiver does not yet know the delivery-count, i.e., delivery-count(rcv) is unspecified, the sender MUST assume that the delivery-count(rcv) is the first delivery-count(snd) sent from sender to receiver, i.e., the delivery-count(snd) specified in the flow state carried by the initial attach frame from the sender to the receiver.

Additionally, whenever the sender increases delivery-count, it MUST decrease link-credit by the same amount in order to maintain the delivery-limit identified by the receiver.

## Interpretation

### Sender

* link-credit initialized to 0
* must wait for flow performative from receiver
* when flow received:
  * link-credit is set to delivery-count(rcv) + link-credit(rcv) - delivery-count

When sending:

* delivery-count is increased by 1
* link-credit is decreased by 1

### Receiver

* must send flow performative before messages can be sent

When receiving:

* delivery-count is increased by 1
* link-credit is decreased by 1