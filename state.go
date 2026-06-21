package main

type Role int

//go:generate go tool stringer -type Role -trimprefix "Role"
const (
	RoleFollower Role = iota
	RoleCandidate
	RoleLeader
)
