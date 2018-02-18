# pack.ag/amqp Changelog

### [0.4.0](https://github.com/vcabbage/amqp/releases/tag/v0.4.0)

Thank you to [@amenzhinsky](https://github.com/amenzhinsky) and [@devigned](https://github.com/devigned) for their contributions to this release!

#### Summary

* Fix channel number handling. ([2210add](https://github.com/vcabbage/amqp/commit/2210add))
* Add [`ConnMaxSession`](https://godoc.org/pack.ag/amqp#ConnMaxSessions) option and default to 65536. ([2210add](https://github.com/vcabbage/amqp/commit/2210add))
* Fix data race on `link.linkCredit`. ([91b4d47](https://github.com/vcabbage/amqp/commit/91b4d47))
* Encode/decode performance optimizations. ([60f2887](https://github.com/vcabbage/amqp/commit/60f2887), [ecc4b67](https://github.com/vcabbage/amqp/commit/ecc4b67), [c1e6bfe](https://github.com/vcabbage/amqp/commit/c1e6bfe), [41f4868](https://github.com/vcabbage/amqp/commit/41f4868), [01403af](https://github.com/vcabbage/amqp/commit/01403af), [7c74332](https://github.com/vcabbage/amqp/commit/7c74332), [88f19d2](https://github.com/vcabbage/amqp/commit/88f19d2), [088f461](https://github.com/vcabbage/amqp/commit/088f461), [27cb779](https://github.com/vcabbage/amqp/commit/27cb779), [7036f82](https://github.com/vcabbage/amqp/commit/7036f82))
* Fix map32 encoding. ([@amenzhinsky](https://github.com/amenzhinsky), [9d0e0f3](https://github.com/vcabbage/amqp/commit/9d0e0f3))
* Fix "amqps" scheme handling. ([@amenzhinsky](https://github.com/amenzhinsky), [746c1d6](https://github.com/vcabbage/amqp/commit/746c1d6))
* Add support for floats, arrays, and lists. ([696ba30](https://github.com/vcabbage/amqp/commit/696ba30))
* Make Go `int` and `uint` encode/decode as AMQP `long` and `ulong` rather than varying based on system architecture. ([696ba30](https://github.com/vcabbage/amqp/commit/696ba30))
* Add support for credentials from URI. ([@amenzhinsky](https://github.com/amenzhinsky), [d62cd7a](https://github.com/vcabbage/amqp/commit/d62cd7a))
* Add separate [`LinkSourceAddress`](https://godoc.org/pack.ag/amqp#LinkSourceAddress) and [`LinkTargetAddress`](https://godoc.org/pack.ag/amqp#LinkTargetAddress) options. Deprecate [`LinkAddress`](https://godoc.org/pack.ag/amqp#LinkAddress). ([@amenzhinsky](https://github.com/amenzhinsky), [156a96c](https://github.com/vcabbage/amqp/commit/156a96c))
* Correctly end sessions when calling [`Session.Close`](https://godoc.org/pack.ag/amqp#Session.Close). ([c27ac98](https://github.com/vcabbage/amqp/commit/c27ac98))
* Fix issue where an incoming frame could be directed to the wrong session. ([fa8dbbf](https://github.com/vcabbage/amqp/commit/fa8dbbf))
* Restrict message annotation keys to `string`, `int`, and `int64`. ([c2a889f](https://github.com/vcabbage/amqp/commit/c2a889f))
* Add descriptions to [`MessageProperties`](https://godoc.org/pack.ag/amqp/#MessageProperties). ([749c2de](https://github.com/vcabbage/amqp/commit/749c2de))
* Minimize delivery-tags. ([53719b2](https://github.com/vcabbage/amqp/commit/53719b2))
* Fix issue where handles may never be cleared from a map when message was sender settled. ([53719b2](https://github.com/vcabbage/amqp/commit/53719b2))
* Fix potential deadlock when session ends before link closes. ([a3e580d](https://github.com/vcabbage/amqp/commit/a3e580d))
* Fix cases where [`Receiver.Receive`](https://godoc.org/pack.ag/amqp/#Receiver.Receive) could return `nil` `Message` and `error`. (Thanks to [@amenzhinsky](https://github.com/amenzhinsky) for reporting, [e2cb6f5](https://github.com/vcabbage/amqp/commit/e2cb6f5))
* Add support for setting connection properties via [`ConnProperty`](https://godoc.org/pack.ag/amqp/#ConnProperty) option. ([@devigned](https://github.com/devigned), [ccafaa7](https://github.com/vcabbage/amqp/commit/ccafaa7))
* Add [`Message.Format`](https://godoc.org/pack.ag/amqp/#Message.Format) field to allow getting/setting the message format. ([54320c8](https://github.com/vcabbage/amqp/commit/54320c8))
* Add support for multiple data sections. ([9c101f5](https://github.com/vcabbage/amqp/commit/9c101f5))

#### Breaking Changes

* [Message.Data](https://godoc.org/pack.ag/amqp#Message.Data) has been changed from `[]byte` to `[][]byte` to support multiple data sections.
  * [`NewMessage`](https://godoc.org/pack.ag/amqp#NewMessage) added to simplify constructing messages with a single data section.
  * [`Message.GetData`](https://godoc.org/pack.ag/amqp#Message.GetData) added to simplify retreiving the first data section from a message.
* [Message.DeliveryAnnotations](https://godoc.org/pack.ag/amqp#Message.DeliveryAnnotations), [Message.Annotations](https://godoc.org/pack.ag/amqp#Message.Annotations), and [Message.Footer](https://godoc.org/pack.ag/amqp#Message.Footer) are now defined as type [Annotations](https://godoc.org/pack.ag/amqp#Annotations). [Annotations](https://godoc.org/pack.ag/amqp#Annotations) only supports keys of type `string`, `int`, and `int64`.
* Port for "amqps" now defaults to 5671 instead of 5672.
* Go `int` and `uint` now always encode/decode as `long` and `ulong`.
* Sessions are now ended when calling [`Session.Close`](https://godoc.org/pack.ag/amqp#Session.Close). Previously this only deallocated the session locally.
* Max number number of sessions per connection changed from 1 to 65536.

### [0.3.0](https://github.com/vcabbage/amqp/releases/tag/v0.3.0)

#### Summary

* Adds support for setting source selector filter with [`LinkSelectorFilter()`](https://godoc.org/pack.ag/amqp#LinkSelectorFilter).

This addition allows clients to communicate [offsets to Azure Event Hubs](http://azure.github.io/amqpnetlite/articles/azure_eventhubs.html#filter).

### [0.2.0](https://github.com/vcabbage/amqp/releases/tag/v0.2.0)

#### Summary

* Support for sending messages.
* Support for sending/receiving "value" payloads.
* Added option `LinkAddressDynamic()`, `Sender.Address()` and `Receiver.Address()` to support dynamic addresses.
* Added options `LinkSenderSettle()` and `LinkReceiverSettle()` to allow for configuring settlement modes.
* Added option `ConnSASLAnonymous()` to enable SASL ANONYMOUS authentication.
* UUID support.
* Added basic integration tests against Microsoft Azure Service Bus.
* Debug logging when built with `debug` build tag.
* Many bug fixes.

#### Breaking Changes

* Option `LinkSource()` renamed to `LinkAddress()`.
* As they are optional, `Message.Header` and `Message.Properties` have been changed to pointers.

### [0.1.0](https://github.com/vcabbage/amqp/releases/tag/v0.1.0)

* Initial release
