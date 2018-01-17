# pack.ag/amqp Changelog

### 0.3.0

#### Summary

* Adds support for setting source selector filter with [`LinkSelectorFilter()`](https://godoc.org/pack.ag/amqp#LinkSelectorFilter).

This addition allows clients to communicate [offsets to Azure Event Hubs](http://azure.github.io/amqpnetlite/articles/azure_eventhubs.html#filter).

### 0.2.0

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

### 0.1.0

* Initial release
