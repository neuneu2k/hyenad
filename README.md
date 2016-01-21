# hyenad
Hyena Messaging Daemon

**Warning: Alpha quality code, not for production use**

## About
 
Hyenad is the central element of the [Hyena Architecture], it's role is to 
* manage application modules
* monitor liveliness
* route messages between modules, taking into account node locality and liveliness
 
## Current status

- [X] Basic routing
- [X] TCP Based local IPC
- [ ] Liveliness monitoring
- [ ] Metrics
- [ ] Metrics publication
- [ ] Routing table updates
- [ ] Docker modules lifecycle management
- [ ] Local module authentication
- [ ] Flow control and stream dropping
- [ ] Quality GoDoc comments
- [ ] TCP based hyenad to hyenad communication
- [ ] High bandwidth local IPC (shared memory or unix sockets)


