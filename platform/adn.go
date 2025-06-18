package platform

import (
	"dmp_distribution/module"
)

type Adn struct{}

func (a *Adn) Distribution(task *module.Distribution, batches []map[string]string) error {
	return nil
}
