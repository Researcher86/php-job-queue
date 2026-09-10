<?php

declare(strict_types=1);

namespace App\Worker;

use App\Delivery\Delivery;
use App\Job\Job;
use App\Job\JobResult;

/**
 * What came back from a worker about one delivery.
 *
 * Two things it carries, for two different reasons.
 *
 * The **delivery**, not just the job, because a job can be in two workers'
 * hands at once - see Delivery. Attributing an answer to a job id would let
 * an expired delivery's late answer be applied to the delivery that
 * replaced it, and it did: the obsolete answer completed a job the live
 * worker was still running.
 *
 * The **nullable result**: null means the worker never answered at all - its
 * socket reached EOF, so the process is gone. "The job failed" and "the
 * worker vanished" need different handling (a NACK counts against attempts
 * and may end in the DLQ; a crash puts the job straight back and replaces
 * the worker), and a JobResult has no way to say the second one.
 */
final readonly class WorkerOutcome
{
    public function __construct(
        private Delivery $delivery,
        // null = the worker died holding this delivery.
        private ?JobResult $result,
    ) {
    }

    public function getDelivery(): Delivery
    {
        return $this->delivery;
    }

    public function getJob(): Job
    {
        return $this->delivery->getJob();
    }

    public function getResult(): ?JobResult
    {
        return $this->result;
    }

    /** Whether the worker died rather than answering. */
    public function isCrash(): bool
    {
        return $this->result === null;
    }
}
