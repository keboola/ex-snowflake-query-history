<?php

namespace Keboola\SnowflakeQueryHistory\Config;

use Keboola\Component\Config\BaseConfigDefinition;
use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;

class ConfigDefinition extends BaseConfigDefinition
{
    protected function getParametersDefinition(): ArrayNodeDefinition
    {
        $parametersNode = parent::getParametersDefinition();

        $parametersNode
            ->children()
                ->scalarNode('host')
                    ->isRequired()
                    ->cannotBeEmpty()
                ->end()
                ->scalarNode('warehouse')
                    ->isRequired()
                    ->cannotBeEmpty()
                ->end()
                ->scalarNode('database')
                    ->isRequired()
                    ->cannotBeEmpty()
                ->end()
                ->scalarNode('user')
                    ->isRequired()
                    ->cannotBeEmpty()
                ->end()
                ->scalarNode('#password')
                ->end()
                ->scalarNode('#keyPair')
                ->end()
            ->end();

        return $parametersNode;
    }
}
