package main

type Machine struct {
	Name string `json:"name"`
	Addr string `json:"addr"`
}

type Config struct {
	Machines          []Machine `json:"machines"`
	ElectionMinStep   int       `json:"election_min_step"`
	ElectionMaxStep   int       `json:"election_max_step"`
	LeaderPingStep    int       `json:"leader_ping_step"`
	LeaderPingTimeout int       `json:"leader_ping_timeout"`
}
