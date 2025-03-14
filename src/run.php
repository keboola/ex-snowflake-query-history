<?php

use Keboola\Component\Logger;
use Keboola\SnowflakeQueryHistory\Component;

require __DIR__ . '/../vendor/autoload.php';

$logger = new Logger();
try {
    $app = new Component($logger);
    $app->execute();
    exit(0);
} catch (\Keboola\Component\UserException $e) {
    $logger->error($e->getMessage());
    exit(1);
}
//catch (\Throwable $e) {
//    $logger->critical(
//        get_class($e) . ':' . $e->getMessage(),
//        [
//            'errFile' => $e->getFile(),
//            'errLine' => $e->getLine(),
//            'errCode' => $e->getCode(),
//            'errTrace' => $e->getTraceAsString(),
//            'errPrevious' => $e->getPrevious() ? get_class($e->getPrevious()) : '',
//        ]
//    );
//    exit(2);
//}


