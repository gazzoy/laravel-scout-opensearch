<?php

declare(strict_types=1);

namespace Zing\LaravelScout\OpenSearch;

use Illuminate\Support\ServiceProvider;
use Laravel\Scout\Builder;
use Laravel\Scout\EngineManager;
use OpenSearch\Client;
use OpenSearch\ClientBuilder;
use Zing\LaravelScout\OpenSearch\Engines\OpenSearchEngine;

class OpenSearchServiceProvider extends ServiceProvider
{
    protected $wheres = [];

    protected ?string $distinctField = null;

    public function boot(): void
    {
        resolve(EngineManager::class)->extend(
            'opensearch',
            static fn (): OpenSearchEngine => new OpenSearchEngine(resolve(Client::class), config(
                'scout.soft_delete',
                false
            ))
        );

        Builder::macro('whereRange', function ($field, string $opRaw, string $value) {
            $ops = collect([
                '>=' => 'gte',
                '<=' => 'lte',
            ]);

            if (! $ops->has($opRaw)) {
                throw new \RuntimeException('Unexpected op:' . $opRaw);
            }

            $op = $ops->get($opRaw);

            if (! isset($this->wheres[$field])) {
                $this->wheres[$field] = [
                    'SCOUT_OPENSEARCH_OP_RANGE' => [],
                ];
            }

            if (! isset($this->wheres[$field]['SCOUT_OPENSEARCH_OP_RANGE'][$op])) {
                $this->wheres[$field]['SCOUT_OPENSEARCH_OP_RANGE'][$op] = $value;
            }

            return $this;
        });

        Builder::macro('distinct', function ($field) {
            $this->distinctField = $field;

            // @phpstan-ignore-next-line
            return $this->engine()
                ->searchAsDistinct($this);
        });

        Builder::macro(
            'count',
            /** @phpstan-ignore-next-line */
            fn () => $this->engine()
                ->getTotalCount(
                    // @phpstan-ignore-next-line
                    $this->engine()
                        ->search($this)
                )
        );
    }

    public function register(): void
    {
        $this->app->singleton(
            Client::class,
            static fn ($app): Client => ClientBuilder::fromConfig($app['config']->get('scout.opensearch.client'))
        );
    }
}
