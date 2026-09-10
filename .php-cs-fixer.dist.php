<?php

declare(strict_types=1);

/**
 * Formatting is a settled question in this repository, not a per-file
 * judgement call: PER-CS 2.0 (PSR-12's successor) plus four rules that
 * match how the code was already written by hand.
 *
 * The point of having a formatter here at all is that the project is meant
 * to be read. Diffs should show a change in behaviour, never a change in
 * brace placement.
 */

$finder = PhpCsFixer\Finder::create()
    ->in([__DIR__ . '/src', __DIR__ . '/tests', __DIR__ . '/bin']);

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
        // Adding an argument to a multiline call should touch one line, not
        // two.
        'trailing_comma_in_multiline' => ['elements' => ['arrays', 'arguments', 'parameters']],
    ])
    ->setFinder($finder);
