<?php

declare(strict_types=1);

namespace PhpJobQueue\Delivery;

use RuntimeException;

/**
 * A delivery ended with no answer at all - the worker died holding the job,
 * its lease expired, or the whole process died with it in flight - and that
 * was the job's last allowed attempt.
 *
 * Nothing threw this in a handler. It exists because a DLQ record states
 * what finished a job, and "nobody ever answered, three times" is a cause
 * worth stating as plainly as an exception from the handler would be. See
 * JobDispatcher::redeliverOrDeadLetter().
 */
final class DeliveryLostException extends RuntimeException
{
}
