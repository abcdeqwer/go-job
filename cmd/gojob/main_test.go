package main

import "testing"

func TestParseTrustedProxyCIDRs(t *testing.T) {
	prefixes, err := parseTrustedProxyCIDRs("X-GoJob-User", "172.19.16.9/32, 10.0.0.0/8")
	if err != nil {
		t.Fatal(err)
	}
	if len(prefixes) != 2 || prefixes[0].String() != "172.19.16.9/32" || prefixes[1].String() != "10.0.0.0/8" {
		t.Fatalf("prefixes = %v", prefixes)
	}
}

func TestParseTrustedProxyCIDRsFailsClosed(t *testing.T) {
	for _, tc := range []struct {
		name       string
		userHeader string
		cidrs      string
	}{
		{name: "header without sources", userHeader: "X-GoJob-User"},
		{name: "sources without header", cidrs: "172.19.16.9/32"},
		{name: "invalid source", userHeader: "X-GoJob-User", cidrs: "not-a-cidr"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := parseTrustedProxyCIDRs(tc.userHeader, tc.cidrs); err == nil {
				t.Fatal("configuration was accepted")
			}
		})
	}
}
