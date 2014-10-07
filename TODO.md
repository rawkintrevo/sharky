# To Do

## Setup
- [ ] Write Wiki and Readme


## Writers
- [X] CSV
- [X] MongoDB
- [ ] If more than one writer is used then all tweets end up going to the first writer. Need to have a handler to repeate tweets to all writers. 

## Streamers
- [ ] Datasift

## Health Checks
- [ ] Canary Test (Make sure data is being written to target)
- [ ] Streamer keeps getting restart signals, but IDK why.  Need to update logging to record why streamer is failing. 
