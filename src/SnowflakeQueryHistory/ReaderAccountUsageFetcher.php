<?php

declare(strict_types=1);

namespace Keboola\SnowflakeQueryHistory;

class ReaderAccountUsageFetcher extends Fetcher
{
    protected function buildQuery(string $start, ?string $end, int $limit): string
    {
        return sprintf(
            <<<'SQL'
                SELECT  
                %s
                FROM SNOWFLAKE.READER_ACCOUNT_USAGE.QUERY_HISTORY
                WHERE end_time >= TO_TIMESTAMP_LTZ('%s')
                    AND end_time <= %s
                ORDER BY end_time DESC
                LIMIT %d
                SQL,
            $this->getQueryColumns(),
            $start,
            $end === null ? 'dateadd(minute, -3, getdate())' : sprintf('TO_TIMESTAMP_LTZ(\'%s\')', $end),
            $limit,
        );
    }
}
