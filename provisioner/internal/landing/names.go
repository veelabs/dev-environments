package landing

import (
	"crypto/rand"
	"math/big"
)

// Word lists for friendly env names (oc-<adjective>-<noun>). Words only — no
// digits — so names can never collide with the port-hostname shape
// (oc-<env>-<port>) that normalizeEnvID rejects.
var (
	adjectives = []string{
		"amber", "bold", "brave", "brisk", "calm", "clever", "cosmic", "crisp",
		"eager", "fable", "fleet", "gentle", "keen", "lively", "lucid", "lunar",
		"mellow", "nimble", "polar", "quick", "rapid", "solar", "spry", "stellar",
		"sunny", "swift", "tidal", "vivid", "wild", "witty", "zesty", "zippy",
	}
	nouns = []string{
		"badger", "condor", "cricket", "dolphin", "falcon", "fox", "gecko",
		"heron", "ibex", "jaguar", "kestrel", "lemur", "lynx", "marmot",
		"marten", "narwhal", "ocelot", "orca", "osprey", "otter", "owl",
		"panda", "puffin", "quokka", "raven", "seal", "swallow", "tapir",
		"toucan", "walrus", "wombat", "wren",
	}
)

// randomName returns a friendly two-word name like "swift-otter". The
// workflow's normalizeEnvID prefixes it to oc-swift-otter.
func randomName() string {
	return pick(adjectives) + "-" + pick(nouns)
}

func pick(words []string) string {
	n, err := rand.Int(rand.Reader, big.NewInt(int64(len(words))))
	if err != nil {
		// crypto/rand failing is unrecoverable weirdness; fall back to word 0
		// rather than crash the claim path.
		return words[0]
	}
	return words[n.Int64()]
}
