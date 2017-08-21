package main

type OutStream interface {
	openOutStream() *chan string
}

type InStream interface {
	openInStream() *chan string
}
