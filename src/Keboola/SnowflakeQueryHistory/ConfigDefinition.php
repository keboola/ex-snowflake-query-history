<?php
/**
 * Created by PhpStorm.
 * User: martinhalamicek
 * Date: 17/07/2017
 * Time: 10:16
 */

namespace  Keboola\SnowflakeQueryHistory;

use Symfony\Component\Config\Definition\Builder\TreeBuilder;
use Symfony\Component\Config\Definition\ConfigurationInterface;

class ConfigDefinition implements ConfigurationInterface
{

    /**
     * Generates the configuration tree builder.
     *
     * @return \Symfony\Component\Config\Definition\Builder\TreeBuilder The tree builder
     */
    public function getConfigTreeBuilder()
    {
        $treeBuilder = new TreeBuilder();
        $rootNode = $treeBuilder->root('parameters');

        $rootNode
            ->children()
                ->arrayNode('db')
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
                            ->isRequired()
                            ->cannotBeEmpty()
                            ->end()
                    ->end()
            ->end();

        return $treeBuilder;
    }
}