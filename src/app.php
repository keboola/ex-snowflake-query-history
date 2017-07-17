<?php
require __DIR__ . '/../vendor/autoload.php';

set_error_handler(function ($errno, $errstr, $errfile, $errline) {
    if (0 === error_reporting()) {
        return false;
    }
    throw new ErrorException($errstr, 0, $errno, $errfile, $errline);
});

use Symfony\Component\Console\Application;
use Symfony\Component\Console\Output\ConsoleOutput;
use Keboola\SnowflakeQueryHistory\RunCommand;

$application = new Application;
$application->add(new RunCommand());
$application->run(null, new ConsoleOutput());
