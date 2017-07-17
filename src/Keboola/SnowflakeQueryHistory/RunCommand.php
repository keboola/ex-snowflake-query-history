<?php
/**
 * Created by PhpStorm.
 * User: martinhalamicek
 * Date: 17/07/2017
 * Time: 09:36
 */

namespace  Keboola\SnowflakeQueryHistory;

use Keboola\Csv\CsvFile;
use Symfony\Component\Config\Definition\Processor;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Serializer\Encoder\JsonDecode;
use Symfony\Component\Serializer\Encoder\JsonEncoder;

class RunCommand extends Command
{
    protected function configure()
    {
        $this->setName('run');
        $this->setDescription('Runs the App');
        $this->addArgument('data directory', InputArgument::REQUIRED, 'Data directory');
    }


    protected function execute(InputInterface $input, OutputInterface $consoleOutput)
    {
        $dataDirectory = $input->getArgument('data directory');

        try {
            $configFilePath = "$dataDirectory/config.json";
            if (!file_exists($configFilePath)) {
                throw new \Exception("Config file not found at path $configFilePath");
            }

            $stateFilePath = "$dataDirectory/in/state.json";
            if (!file_exists($stateFilePath)) {
                throw new \Exception("State file not found at path $stateFilePath");
            }

            $decode = new JsonDecode(true);
            $decoded = $decode->decode(file_get_contents($configFilePath), JsonEncoder::FORMAT);

            $processor = new Processor();
            $parameters = $processor->processConfiguration(new ConfigDefinition(), [isset($decoded['parameters']) ? $decoded['parameters'] : []]);

            $consoleOutput->writeln("Fetching query history from {$parameters['host']}");

            $connection = new \Keboola\Db\Import\Snowflake\Connection([
                'host' => $parameters['host'],
                'user' => $parameters['user'],
                'password' => $parameters['#password'],
                'database' => $parameters['database'],
                'warehouse' => $parameters['warehouse'],
            ]);
            $connection->query('alter session set timezone = \'UTC\'');

            $fetcher = new \Keboola\SnowflakeQueryHistory\Fetcher($connection);

            (new Filesystem())->mkdir("$dataDirectory/out/tables");
            $queriesCsvFile = new CsvFile("$dataDirectory/out/tables/queries.csv");

            $stats = [
                'latestEndTime' => null,
                'rowsFetched' => 0,
            ];

            $fetcher->fetchHistory(function ($queryRow, $rowNumber) use ($consoleOutput, $queriesCsvFile, $dataDirectory, &$stats) {
                if ($rowNumber === 0) {
                    // write header
                    $queriesCsvFile->writeRow(array_keys($queryRow));
                    // most recent query
                    $stats['latestEndTime'] =$queryRow['END_TIME'];
                }

                if ($rowNumber > 0 && $rowNumber % 10000 === 0) {
                    $consoleOutput->writeln(sprintf("%d queries fetched total", $rowNumber));
                }

                $stats['rowsFetched'] = $rowNumber;
                $queriesCsvFile->writeRow($queryRow);
            });
            $consoleOutput->writeln(sprintf("%d queries fetched total", $stats['rowsFetched']));

            // write state
            (new Filesystem())->dumpFile("$dataDirectory/out/state.json", json_encode([
                'latestEndTime' => $stats['latestEndTime'],
            ]));

            // write manifest
            (new Filesystem())->dumpFile("$dataDirectory/out/tables/queries.csv.manifest", json_encode([
                'primary_key' => 'QUERY_ID',
            ]));

            return 0;
        } catch (\Exception $e) {
            $consoleOutput->writeln("{$e->getMessage()}\n{$e->getTraceAsString()}");
            return 2;
        }
    }
}
