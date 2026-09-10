<?php

declare(strict_types=1);

namespace App\Tests\Support;

use App\Delivery\Delivery;
use App\Job\Job;
use App\Worker\Worker;

/**
 * Leases for tests that drive a Worker directly.
 *
 * A Delivery normally comes from VisibilityMonitor::track(), which is what
 * makes it the one entitled to answer. A test working a worker on its own
 * has no monitor, so it mints its own - and having to say so is the point:
 * a worker is handed a lease, not a job.
 */
final class Deliveries
{
    public static function to(Worker $worker, Job $job): Delivery
    {
        return Delivery::of($job, $worker->getId(), microtime(true));
    }
}
