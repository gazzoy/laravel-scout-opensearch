<?php

declare(strict_types=1);

namespace Zing\LaravelScout\OpenSearch\Engines;

use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\SoftDeletes;
use Illuminate\Support\Arr;
use Illuminate\Support\Collection;
use Illuminate\Support\LazyCollection;
use Laravel\Scout\Builder;
use Laravel\Scout\Engines\Engine;
use Laravel\Scout\Jobs\RemoveableScoutCollection;
use OpenSearch\Client;

/**
 * @mixin \OpenSearch\Client
 */
class OpenSearchEngine extends Engine
{
    /**
     * Create a new engine instance.
     */
    public function __construct(
        protected Client $client,
        protected bool $softDelete = false
    ) {
    }

    /**
     * Update the given model in the index.
     *
     * @param \Illuminate\Database\Eloquent\Collection $models
     */
    public function update($models): void
    {
        if ($models->isEmpty()) {
            return;
        }

        /** @var \Illuminate\Database\Eloquent\Model $model First model for search index */
        $model = $models->first();
        if ($this->usesSoftDelete($model) && $this->softDelete) {
            $models->each->pushSoftDeleteMetadata();
        }

        $objects = $models->map(static function ($model): ?array {
            $searchableData = $model->toSearchableArray();
            if (empty($searchableData)) {
                return null;
            }

            return array_merge($searchableData, $model->scoutMetadata(), [
                $model->getScoutKeyName() => $model->getScoutKey(),
            ]);
        })->filter()
            ->values()
            ->all();

        if ($objects !== []) {
            $data = [];
            foreach ($objects as $object) {
                $data[] = [
                    'index' => [
                        '_index' => $model->searchableAs(),
                        '_id' => $object[$model->getScoutKeyName()],
                    ],
                ];
                $data[] = $object;
            }

            $this->client->bulk([
                'index' => $model->searchableAs(),
                'body' => $data,
            ]);
        }
    }

    /**
     * Remove the given model from the index.
     *
     * @param \Illuminate\Database\Eloquent\Collection $models
     */
    public function delete($models): void
    {
        if ($models->isEmpty()) {
            return;
        }

        /** @var \Illuminate\Database\Eloquent\Model $model */
        $model = $models->first();

        $keys = $models instanceof RemoveableScoutCollection
            ? $models->pluck($model->getScoutKeyName())
            : $models->map->getScoutKey();

        $data = $keys->map(static fn ($object): array => [
            'delete' => [
                '_index' => $model->searchableAs(),
                '_id' => $object,
            ],
        ])->all();

        $this->client->bulk([
            'index' => $model->searchableAs(),
            'body' => $data,
        ]);
    }

    /**
     * Perform the given search on the engine.
     */
    public function search(Builder $builder): mixed
    {
        $options = $this->getOptions($builder, [
            '_source' => true,
            'size' => $builder->limit ?: 10000,
            'from' => 0,
        ]);
        $options['query'] = $this->filters($builder);

        return $this->performSearch($builder, $options);
    }

    public function searchAsDistinct(Builder $builder): Collection
    {
        $results = $this->search($builder);

        // @phpstan-ignore-next-line
        if (Arr::has($results, sprintf('aggregations.%s.buckets', $builder->distinctField))) {
            // @phpstan-ignore-next-line
            return collect(Arr::get($results, sprintf('aggregations.%s.buckets', $builder->distinctField)))->pluck(
                'key'
            );
        }

        return collect();
    }

    /**
     * Perform the given search on the engine.
     *
     * @param int $perPage
     * @param int $page
     */
    public function paginate(Builder $builder, $perPage, $page): mixed
    {
        return $this->performSearch($builder, array_filter([
            '_source' => true,
            'query' => $this->filters($builder),
            'size' => $perPage ?: 10,
            'from' => ($page - 1) * $perPage,
        ]));
    }

    /**
     * Perform the given search on the engine.
     *
     * @param array<string, mixed> $options
     */
    protected function performSearch(Builder $builder, array $options = []): mixed
    {
        $index = $builder->index ?: $builder->model->searchableAs();
        if (property_exists($builder, 'options')) {
            $options = array_merge($builder->options, $options);
        }

        if ($builder->callback instanceof \Closure) {
            return \call_user_func($builder->callback, $this->client, $builder->query, $options);
        }

        $query = $builder->query;
        $must = collect([
            [
                'query_string' => [
                    'query' => $query,
                ],
            ],
        ]);
        $must = $must->merge(collect($builder->wheres)
            ->map(static fn ($value, $key): array => [
                'term' => [
                    $key => $value,
                ],
            ])->values())->values();

        if (property_exists($builder, 'whereIns')) {
            $must = $must->merge(collect($builder->whereIns)->map(static fn ($values, $key): array => [
                'terms' => [
                    $key => $values,
                ],
            ])->values())->values();
        }

        $mustNot = collect();
        if (property_exists($builder, 'whereNotIns')) {
            $mustNot = $mustNot->merge(collect($builder->whereNotIns)->map(static fn ($values, $key): array => [
                'terms' => [
                    $key => $values,
                ],
            ])->values())->values();
        }

        $options['query'] = [
            'bool' => [
                'must' => $must->all(),
                'must_not' => $mustNot->all(),
            ],
        ];

        $options['sort'] = collect($builder->orders)->map(static fn ($order): array => [
            $order['column'] => [
                'order' => $order['direction'],
            ],
        ])->all();
        $result = $this->client->search([
            'index' => $index,
            'body' => $options,
        ]);

        return $result['hits'] ?? null;
    }

    /**
     * Pluck and return the primary keys of the given results.
     *
     * @param array{hits: mixed[]|null}|null $results
     */
    public function mapIds($results): Collection
    {
        if ($results === null) {
            return collect();
        }

        return collect($results['hits'])->pluck('_id')->values();
    }

    /**
     * Map the given results to instances of the given model.
     *
     * @param array{hits: mixed[]|null}|null $results
     * @param \Illuminate\Database\Eloquent\Model $model
     *
     * @return \Illuminate\Database\Eloquent\Collection
     */
    public function map(Builder $builder, $results, $model): mixed
    {
        if ($results === null) {
            return $model->newCollection();
        }

        if (! isset($results['hits'])) {
            return $model->newCollection();
        }

        if ($results['hits'] === []) {
            return $model->newCollection();
        }

        $objectIds = collect($results['hits'])->pluck('_id')->values()->all();

        $objectIdPositions = array_flip($objectIds);

        return $model->getScoutModelsByIds($builder, $objectIds)
            ->filter(static fn ($model): bool => \in_array($model->getScoutKey(), $objectIds, false))
            ->sortBy(static fn ($model): int => $objectIdPositions[$model->getScoutKey()])->values();
    }

    /**
     * Map the given results to instances of the given model via a lazy collection.
     *
     * @param array{hits: mixed[]|null}|null $results
     * @param \Illuminate\Database\Eloquent\Model $model
     */
    public function lazyMap(Builder $builder, $results, $model): LazyCollection
    {
        if ($results === null) {
            return LazyCollection::make($model->newCollection());
        }

        if (! isset($results['hits'])) {
            return LazyCollection::make($model->newCollection());
        }

        if ($results['hits'] === []) {
            return LazyCollection::make($model->newCollection());
        }

        $objectIds = collect($results['hits'])->pluck('_id')->values()->all();
        $objectIdPositions = array_flip($objectIds);

        return $model->queryScoutModelsByIds($builder, $objectIds)
            ->cursor()
            ->filter(static fn ($model): bool => \in_array($model->getScoutKey(), $objectIds, false))
            ->sortBy(static fn ($model): int => $objectIdPositions[$model->getScoutKey()])->values();
    }

    /**
     * Get the total count from a raw result returned by the engine.
     *
     * @param mixed $results
     */
    public function getTotalCount($results): int
    {
        return $results['total']['value'] ?? 0;
    }

    /**
     * Flush all of the model's records from the engine.
     *
     * @param \Illuminate\Database\Eloquent\Model $model
     */
    public function flush($model): void
    {
        $this->client->deleteByQuery([
            'index' => $model->searchableAs(),
            'body' => [
                'query' => [
                    'match_all' => new \stdClass(),
                ],
            ],
        ]);
    }

    /**
     * Create a search index.
     *
     * @param string $name
     * @param array<string, mixed> $options
     *
     * @return array{acknowledged: bool, shards_acknowledged: bool, index: string}
     *
     * @phpstan-return array<string, mixed>
     */
    public function createIndex($name, array $options = []): array
    {
        $body = array_replace_recursive(
            config('scout.opensearch.indices.default') ?? [],
            config('scout.opensearch.indices.' . $name) ?? []
        );

        return $this->client->indices()
            ->create([
                'index' => $name,
                'body' => $body,
            ]);
    }

    /**
     * Delete a search index.
     *
     * @param string $name
     *
     * @return array{acknowledged: bool}
     *
     * @phpstan-return array<string, mixed>
     */
    public function deleteIndex($name): array
    {
        return $this->client->indices()
            ->delete([
                'index' => $name,
            ]);
    }

    /**
     * Determine if the given model uses soft deletes.
     */
    protected function usesSoftDelete(Model $model): bool
    {
        return \in_array(SoftDeletes::class, class_uses_recursive($model), true);
    }

    /**
     * Dynamically call the OpenSearch client instance.
     *
     * @param string $method
     * @param array<int, mixed> $parameters
     *
     * @return mixed
     */
    public function __call($method, $parameters)
    {
        return $this->client->{$method}(...$parameters);
    }

    private function getOptions(Builder $builder, array $options): array
    {
        if (property_exists($builder, 'distinctField') && filled($builder->distinctField)) {
            return [
                'stored_fields' => $builder->distinctField,
                'aggregations' => [
                    $builder->distinctField => [
                        'terms' => [
                            'field' => $builder->distinctField . '.raw',
                            'size' => 200,
                            'min_doc_count' => 1,
                            'shard_min_doc_count' => 0,
                            'show_term_doc_count_error' => false,
                            'order' => [
                                '_count' => 'desc',
                                '_key' => 'asc',
                            ],
                        ],
                    ],
                ],
            ];
        }

        return $options;
    }

    private function filters(Builder $builder): array
    {
        $query = [];

        if ($builder->query !== '' && $builder->query !== '0') {
            /** @phpstan-ignore-line */
            $fields = $builder->model->searchableFields();

            $query['bool'] = [
                'must' => [
                    [
                        'simple_query_string' => [
                            'query' => $builder->query,
                            'fields' => $fields,
                            'default_operator' => 'and',
                        ],
                    ],
                ],
            ];
        }

        if (\count($builder->wheres) > 0) {
            $wheres = array_merge([
                '__soft_deleted' => 0,
            ], $builder->wheres);

            foreach ($wheres as $key => $value) {
                if (\is_array($value) && isset($value['SCOUT_OPENSEARCH_OP_RANGE'])) {
                    $range = $value['SCOUT_OPENSEARCH_OP_RANGE'];
                    $query['bool']['filter'][] = [
                        'range' => [
                            $key => $range,
                        ],
                    ];
                } else {
                    $query['bool']['filter'][] = [
                        'term' => [
                            $key => $value,
                        ],
                    ];
                }
            }
        }// end if
        // end if

        if (\count($builder->whereIns) > 0) {
            $query['bool']['minimum_should_match'] = \count($builder->whereIns);
            $query['bool']['should'] = [];
            foreach ($builder->whereIns as $key => $values) {
                foreach ($values as $value) {
                    $query['bool']['should'][] = [
                        'term' => [
                            $key => $value,
                        ],
                    ];
                }
            }
        }

        return $query;
    }
}
