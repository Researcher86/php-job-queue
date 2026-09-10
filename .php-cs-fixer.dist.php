<?php

declare(strict_types=1);

/**
 * Formatting is a settled question in this repository, not a per-file
 * judgement call: PER-CS 2.0 (PSR-12's successor) plus five rules that
 * match how the code was already written by hand.
 *
 * The point of having a formatter here at all is that the project is meant
 * to be read. Diffs should show a change in behaviour, never a change in
 * brace placement.
 *
 * Two conventions no rule can express, so they are written down here:
 *
 *  - A constructor's parameters go one per line with a trailing comma, even
 *    when they would fit on one. Adding or removing a dependency is then a
 *    one-line diff, and a promoted property can carry its own comment.
 *  - Dependencies are promoted, with their default in the signature
 *    (`private readonly Clock $clock = new SystemClock()`), rather than
 *    taken as nullable and resolved in the body. The signature is then the
 *    whole truth about what the object holds, and `readonly` goes on
 *    everything that is never reassigned.
 *
 * The one thing that is NOT promoted is a collaborator the object owns
 * outright and nobody may substitute - PriorityQueue's DelayedJobScheduler,
 * the scheduler's own heap. Those are built in the body, because putting
 * them in the signature would advertise that two queues could be made to
 * share one, which would be a bug rather than a configuration.
 */

$finder = PhpCsFixer\Finder::create()
    ->in([__DIR__ . '/src', __DIR__ . '/tests', __DIR__ . '/bin', __DIR__ . '/examples']);

return (new PhpCsFixer\Config())
    ->setRiskyAllowed(true)
    ->setRules([
        '@PER-CS2.0' => true,
        // Every file in src/ and tests/ already declares it; the fixer is
        // what keeps a new file from forgetting.
        'declare_strict_types' => true,
        // Global classes are imported (`use LogicException;`) rather than
        // written as `\LogicException` inline - the reference project's
        // convention, and it keeps the dependencies of a file visible at
        // the top of it.
        'global_namespace_import' => [
            'import_classes' => true,
            'import_constants' => false,
            'import_functions' => false,
        ],
        'ordered_imports' => ['sort_algorithm' => 'alpha'],
        // PER-CS wants `fn(`; this codebase and php-worker-pool both write
        // `fn (`, so the existing spelling wins over the preset.
        'function_declaration' => ['closure_fn_spacing' => 'one'],
        // PER-CS collapses an empty body to `) {}`. A constructor here is
        // usually nothing BUT promoted properties, and `) {` with the brace
        // on its own line keeps the parameter list reading as the body it
        // effectively is - which is how php-worker-pool writes them too.
        'single_line_empty_body' => false,
        // Adding an argument to a multiline call should touch one line, not
        // two.
        'trailing_comma_in_multiline' => ['elements' => ['arrays', 'arguments', 'parameters']],
    ])
    ->setFinder($finder);
