package main

type Role int

const (
	RoleFollower Role = iota
	RoleCandidate
	RoleLeader
)
