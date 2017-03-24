package clinsq

import (
	"errors"
	"fmt"

	"github.com/golang/glog"
	"gopkg.in/urfave/cli.v2"

	"github.com/crackcomm/cli-flags"
	"github.com/crackcomm/nsqueue/consumer"
	"github.com/crackcomm/nsqueue/producer"
)

// AddrFlag - NSQ address command line flag.
var AddrFlag = &cli.StringSliceFlag{
	Name:    "nsq-addr",
	EnvVars: []string{"NSQ_ADDR"},
	Usage:   "nsq address",
}

// LookupAddrFlag - NSQ lookup address command line flag.
var LookupAddrFlag = &cli.StringSliceFlag{
	Name:    "nsqlookup-addr",
	EnvVars: []string{"NSQLOOKUP_ADDR"},
	Usage:   "nsqlookup address",
}

// TopicFlag - NSQ topic command line flag.
var TopicFlag = &cli.StringSliceFlag{
	Name:    "nsq-topic",
	EnvVars: []string{"NSQ_TOPIC"},
	Usage:   "nsq topic",
}

// ChannelFlag - NSQ channel command line flag.
var ChannelFlag = &cli.StringFlag{
	Name:    "nsq-channel",
	EnvVars: []string{"NSQ_CHANNEL"},
	Usage:   "nsq channel",
	Value:   "consumer",
}

// Connect - Connects to NSQ from nsq-addr and nsqlookup-addr flags.
func Connect(c *cli.Context) (err error) {
	// Get list of nsq addresses
	nsqAddrs := c.StringSlice(AddrFlag.Name)
	if len(nsqAddrs) == 0 {
		return errors.New("at least one --nsq-addr is required")
	}

	// Connect producer to first nsq address
	nsqAddr := nsqAddrs[0]
	if err := producer.Connect(nsqAddr); err != nil {
		return fmt.Errorf("error connecting producer to %q: %v", nsqAddr, err)
	}

	// Connect consumer to all nsq addresses
	for _, addr := range nsqAddrs {
		glog.V(2).Infof("Connecting to nsq %s", addr)
		if err := consumer.Connect(addr); err != nil {
			return fmt.Errorf("error connecting to nsq %q: %v", addr, err)
		}
		glog.V(2).Infof("Connected to nsq %s", addr)
	}

	// Connect consumer to all nsqlookup addresses
	for _, addr := range c.StringSlice("nsqlookup-addr") {
		glog.V(2).Infof("Connecting to nsq lookup %s", addr)
		if err := consumer.ConnectLookupd(addr); err != nil {
			return fmt.Errorf("error connecting to nsq lookup %q: %v", addr, err)
		}
		glog.V(2).Infof("Connected to nsq lookup %s", addr)
	}
	return
}

// RequireAll - Checks for all flags and reports errors if some are missing.
func RequireAll(c *cli.Context) error {
	return cliflags.RequireAll(c, []cli.Flag{
		AddrFlag,
		LookupAddrFlag,
		TopicFlag,
		ChannelFlag,
	})
}
