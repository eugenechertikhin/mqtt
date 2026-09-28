# mqtt server

Review codebase 3 year later. Code is not well. But anyway I can made several fixes.

## Status

Server in beta stage. Most function is works fine. But some todo still exists.

## Purpose

This is my implementation of mqtt server for Maja Suite project. Mqtt server will be central queue for all messages from connected devices and hub.

Managers (wifi, bluetooth or zigbee) will connect to mqtt server at start and push all messages to mqtt. So, interconnect beetween devices different types will be managed by mqtt server.

Hub will be responsible only for automation, receive messages, doing some business logic and and push result messages back.

## Save data on restart

Server use sqlite database to store username/password as well as will/retain messages. So restarts should be clear.

## Code

I write code as simple as possible, so it should (I hope) supported very easy. May be somewhere it looks not very professional, in this case kindly drop me message or pull request (if you can).

Surelly some errors may exists. Use it at your own risk ;)

## Notice

Specification: https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html

Author: Eugene Chertikhin <e.chertikhin@crestwavetech.com>

Licensed under GNU GPL.
