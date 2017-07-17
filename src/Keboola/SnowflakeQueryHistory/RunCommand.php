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
        $consoleOutput->writeln($input->getArgument('data directory'));

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
            $configuration = $processor->processConfiguration(new ConfigDefinition(), [$decoded]);

            $connection = new \Keboola\Db\Import\Snowflake\Connection([
                'host' => $configuration['parameters']['db']['host'],
                'user' => $configuration['parameters']['db']['user'],
                'password' => $configuration['parameters']['db']['#password'],
                'database' => $configuration['parameters']['db']['database'],
                'warehouse' => $configuration['parameters']['db']['warehouse'],
            ]);
            $connection->query('alter session set timezone = \'UTC\'');

            $fetcher = new \Keboola\SnowflakeQueryHistory\Fetcher($connection);

            (new Filesystem())->mkdir("$dataDirectory/out/tables");
            $queriesCsvFile = new CsvFile("$dataDirectory/out/tables/queries.csv");

            $fetcher->fetchHistory(function ($queryRow, $rowNumber) use ($consoleOutput, $queriesCsvFile, $dataDirectory) {
                if ($rowNumber === 0) {
                    // write header
                    $queriesCsvFile->writeRow(array_keys($queryRow));

                    // most recent query
                    (new Filesystem())->dumpFile("$dataDirectory/out/state.json", json_encode([
                        'latestEndTime' => $queryRow['END_TIME'],
                    ]));
                }
                $queriesCsvFile->writeRow($queryRow);
            });

            return 0;
        } catch (\Exception $e) {
            $consoleOutput->writeln("{$e->getMessage()}\n{$e->getTraceAsString()}");
            return 2;
        }
    }
}