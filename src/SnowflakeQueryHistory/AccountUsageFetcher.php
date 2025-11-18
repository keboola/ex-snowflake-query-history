<?php

declare(strict_types=1);

namespace Keboola\SnowflakeQueryHistory;

class AccountUsageFetcher extends Fetcher
{
    protected function buildQuery(string $start, ?string $end, int $limit): string
    {
        return sprintf(
            <<<'SQL'
                SELECT  
                %s
                FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(
                    END_TIME_RANGE_START => TO_TIMESTAMP_LTZ('%s'),
                    END_TIME_RANGE_END => %s,
                    RESULT_LIMIT => %d
                ))
                ORDER BY end_time DESC
                SQL,
            $this->getQueryColumns(),
            $start,
            $end === null ? 'dateadd(minute, -3, getdate())' : sprintf('TO_TIMESTAMP_LTZ(\'%s\')', $end),
            $limit,
        );
    }
}
