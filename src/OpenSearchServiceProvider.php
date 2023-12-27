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
    /**
     * @var array<mixed, array<'gte'|'lte', mixed>>
     */
    public $whereBetween;

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

        Builder::macro('whereBetween', function ($field, array $valueFromTo) {
            if (\count($valueFromTo) !== 2) {
                throw new \RuntimeException('Unexpected value:' . implode(', ', $valueFromTo));
            }

            $this->whereBetween[$field] = [
                'gte' => $valueFromTo[0],
                'lte' => $valueFromTo[1],
            ];

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
