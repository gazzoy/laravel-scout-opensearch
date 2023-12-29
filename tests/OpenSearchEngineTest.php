<?php

declare(strict_types=1);

namespace Zing\LaravelScout\OpenSearch\Tests;

use Illuminate\Container\Container;
use Illuminate\Database\Eloquent\Collection;
use Illuminate\Foundation\Testing\DatabaseTransactions;
use Illuminate\Support\Arr;
use Illuminate\Support\Facades\Config;
use Illuminate\Support\LazyCollection;
use Laravel\Scout\Builder;
use Laravel\Scout\EngineManager;
use Laravel\Scout\Jobs\RemoveFromSearch;
use Mockery as m;
use OpenSearch\Client;
use Zing\LaravelScout\OpenSearch\Engines\OpenSearchEngine;
use Zing\LaravelScout\OpenSearch\Tests\Fixtures\CustomKeySearchableModel;
use Zing\LaravelScout\OpenSearch\Tests\Fixtures\EmptySearchableModel;
use Zing\LaravelScout\OpenSearch\Tests\Fixtures\SearchableAndSoftDeletesModel;
use Zing\LaravelScout\OpenSearch\Tests\Fixtures\SearchableModel;
use Zing\LaravelScout\OpenSearch\Tests\Fixtures\SoftDeletedEmptySearchableModel;

/**
 * @internal
 */
final class OpenSearchEngineTest extends TestCase
{
    use DatabaseTransactions;

    public ?string $distinctField = null;

    /**
     * @var array<mixed, array<'gte'|'lte', mixed>>
     */
    public $whereBetween;

    protected function setUp(): void
    {
        Config::shouldReceive('get')->with('scout.after_commit', m::any())->andReturn(false);
        Config::shouldReceive('get')->with('scout.soft_delete', m::any())->andReturn(false);
    }

    public function testUpdateAddsObjectsToIndex(): void
    {
        $client = m::mock(Client::class);
        $searchableModel = new SearchableModel([
            'id' => 1,
        ]);
        $client->shouldReceive('bulk')
            ->once()
            ->with([
                'index' => 'table',
                'body' => [
                    [
                        'index' => [
                            '_index' => 'table',
                            '_id' => 1,
                        ],
                    ],
                    [
                        'id' => 1,
                        $searchableModel->getScoutKeyName() => $searchableModel->getScoutKey(),
                    ],
                ],
            ]);

        $openSearchEngine = new OpenSearchEngine($client);
        $openSearchEngine->update(Collection::make([$searchableModel]));
    }

    public function testUpdateWithSoftDeletes(): void
    {
        $client = m::mock(Client::class);
        $searchableAndSoftDeletesModel = new SearchableAndSoftDeletesModel([
            'id' => 1,
        ]);
        $client->shouldReceive('bulk')
            ->once()
            ->with([
                'index' => 'table',
                'body' => [
                    [
                        'index' => [
                            '_index' => 'table',
                            '_id' => 1,
                        ],
                    ],
                    [
                        'id' => 1,
                        '__soft_deleted' => 0,
                        $searchableAndSoftDeletesModel->getScoutKeyName() => $searchableAndSoftDeletesModel->getScoutKey(),
                    ],
                ],
            ]);

        $openSearchEngine = new OpenSearchEngine($client, true);
        $openSearchEngine->update(Collection::make([$searchableAndSoftDeletesModel]));
    }

    public function testUpdateEmpty(): void
    {
        $client = m::mock(Client::class);
        $client->shouldNotReceive('bulk');

        $openSearchEngine = new OpenSearchEngine($client);
        $openSearchEngine->update(Collection::make([]));
    }

    public function testDeleteEmpty(): void
    {
        $client = m::mock(Client::class);
        $client->shouldNotReceive('bulk');

        $openSearchEngine = new OpenSearchEngine($client);
        $openSearchEngine->delete(Collection::make([]));
    }

    public function testDeleteRemovesObjectsToIndex(): void
    {
        $client = m::mock(Client::class);
        $client->shouldReceive('bulk')
            ->once()
            ->with([
                'index' => 'table',
                'body' => [
                    [
                        'delete' => [
                            '_index' => 'table',
                            '_id' => 1,
                        ],
                    ],
                ],
            ]);

        $openSearchEngine = new OpenSearchEngine($client);
        $openSearchEngine->delete(Collection::make([
            new SearchableModel([
                'id' => 1,
            ]),
        ]));
    }

    public function testDeleteRemovesObjectsToIndexWithACustomSearchKey(): void
    {
        $client = m::mock(Client::class);
        $client->shouldReceive('bulk')
            ->once()
            ->with([
                'index' => 'table',
                'body' => [
                    [
                        'delete' => [
                            '_index' => 'table',
                            '_id' => 'my-opensearch-key.5',
                        ],
                    ],
                ],
            ]);

        $openSearchEngine = new OpenSearchEngine($client);
        $openSearchEngine->delete(Collection::make([
            new CustomKeySearchableModel([
                'id' => 5,
            ]),
        ]));
    }

    public function testDeleteWithRemoveableScoutCollectionUsingCustomSearchKey(): void
    {
        if (! class_exists(RemoveFromSearch::class)) {
            $this->markTestSkipped('Support for RemoveFromSearch available since 9.0.');
        }

        $job = new RemoveFromSearch(Collection::make([
            new CustomKeySearchableModel([
                'id' => 5,
            ]),
        ]));

        $job = unserialize(serialize($job));

        $client = m::mock(Client::class);
        $client->shouldReceive('bulk')
            ->once()
            ->with([
                'index' => 'table',
                'body' => [
                    [
                        'delete' => [
                            '_index' => 'table',
                            '_id' => 'my-opensearch-key.5',
                        ],
                    ],
                ],
            ]);
        $openSearchEngine = new OpenSearchEngine($client);
        $openSearchEngine->delete($job->models);
    }

    public function testRemoveFromSearchJobUsesCustomSearchKey(): void
    {
        if (! class_exists(RemoveFromSearch::class)) {
            $this->markTestSkipped('Support for RemoveFromSearch available since 9.0.');
        }

        $job = new RemoveFromSearch(Collection::make([
            new CustomKeySearchableModel([
                'id' => 5,
            ]),
        ]));

        $job = unserialize(serialize($job));

        Container::getInstance()->bind(EngineManager::class, static function () {
            $engine = m::mock(OpenSearchEngine::class);
            $engine->shouldReceive('delete')
                ->once()
                ->with(m::on(static function ($collection): bool {
                    $keyName = ($model = $collection->first())
                        ->getScoutKeyName();

                    return $model->getAttributes()[$keyName] === 'my-opensearch-key.5';
                }));
            $manager = m::mock(EngineManager::class);
            $manager->shouldReceive('engine')
                ->once()
                ->andReturn($engine);

            return $manager;
        });

        $job->handle();
    }

    public function testSearchSendsCorrectParametersToAlgolia(): void
    {
        $client = m::mock(Client::class);
        $client->shouldReceive('search')
            ->once()
            ->with([
                'index' => 'table',
                'body' => [
                    'query' => [
                        'bool' => [
                            'must' => [
                                [
                                    'query_string' => [
                                        'query' => 'zonda',
                                    ],
                                ],
                                [
                                    'term' => [
                                        'foo' => 1,
                                    ],
                                ],
                            ],
                            'must_not' => [],
                        ],
                    ],
                    'sort' => [
                        [
                            'id' => [
                                'order' => 'desc',
                            ],
                        ],
                    ],
                ],
            ]);

        $openSearchEngine = new OpenSearchEngine($client);
        $builder = new Builder(new SearchableModel(), 'zonda');
        $builder->where('foo', 1)
            ->orderBy('id', 'desc');
        $openSearchEngine->search($builder);
    }

    public function testSearchSendsCorrectParametersToAlgoliaForWhereInSearch(): void
    {
        if (! method_exists(Builder::class, 'whereIn')) {
            $this->markTestSkipped('Support for whereIn available since 9.0.');
        }

        $client = m::mock(Client::class);
        $client->shouldReceive('search')
            ->once()
            ->with([
                'index' => 'table',
                'body' => [
                    'query' => [
                        'bool' => [
                            'must' => [
                                [
                                    'query_string' => [
                                        'query' => 'zonda',
                                    ],
                                ],
                                [
                                    'term' => [
                                        'foo' => 1,
                                    ],
                                ],
                                [
                                    'terms' => [
                                        'bar' => [1, 2],
                                    ],
                                ],
                            ],
                            'must_not' => [],
                        ],
                    ],
                    'sort' => [
                        [
                            'id' => [
                                'order' => 'desc',
                            ],
                        ],
                    ],
                ],
            ]);

        $openSearchEngine = new OpenSearchEngine($client);
        $builder = new Builder(new SearchableModel(), 'zonda');
        $builder->where('foo', 1)
            ->whereIn('bar', [1, 2])
            ->orderBy('id', 'desc');
        $openSearchEngine->search($builder);
    }

    public function testSearchSendsCorrectParametersToAlgoliaForEmptyWhereInSearch(): void
    {
        if (! method_exists(Builder::class, 'whereIn')) {
            $this->markTestSkipped('Support for whereIn available since 9.0.');
        }

        $client = m::mock(Client::class);
        $client->shouldReceive('search')
            ->once()
            ->with([
                'index' => 'table',
                'body' => [
                    'query' => [
                        'bool' => [
                            'must' => [
                                [
                                    'query_string' => [
                                        'query' => 'zonda',
                                    ],
                                ],
                                [
                                    'term' => [
                                        'foo' => 1,
                                    ],
                                ],
                                [
                                    'terms' => [
                                        'bar' => [],
                                    ],
                                ],
                            ],
                            'must_not' => [],
                        ],
                    ],
                    'sort' => [
                        [
                            'id' => [
                                'order' => 'desc',
                            ],
                        ],
                    ],
                ],
            ]);

        $openSearchEngine = new OpenSearchEngine($client);
        $builder = new Builder(new SearchableModel(), 'zonda');
        $builder->where('foo', 1)
            ->whereIn('bar', [])
            ->orderBy('id', 'desc');
        $openSearchEngine->search($builder);
    }

    public function testSearchSendsCorrectParametersToAlgoliaForWhereBetweenSearch(): void
    {
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

        $client = m::mock(Client::class);
        $client->shouldReceive('search')
            ->once()
            ->with([
                'index' => 'table',
                'body' => [
                    'query' => [
                        'bool' => [
                            'must' => [
                                [
                                    'query_string' => [
                                        'query' => 'zonda',
                                    ],
                                ],
                                [
                                    'term' => [
                                        'foo' => 1,
                                    ],
                                ],
                            ],
                            'must_not' => [],
                            'filter' => [
                                [
                                    'range' => [
                                        'bar' => [
                                            'gte' => 1,
                                            'lte' => 2,
                                        ],
                                    ],
                                ],
                            ],
                        ],
                    ],
                    'sort' => [
                        [
                            'id' => [
                                'order' => 'desc',
                            ],
                        ],
                    ],
                ],
            ]);

        $openSearchEngine = new OpenSearchEngine($client);
        $builder = new Builder(new SearchableModel(), 'zonda');
        $builder->where('foo', 1)
            ->whereBetween('bar', [1, 2])
            ->orderBy('id', 'desc');
        $openSearchEngine->search($builder);
    }

    public function testSearchSendsCorrectParametersToAlgoliaForDistinctSearch(): void
    {
        $client = m::mock(Client::class);
        $client->shouldReceive('search')
            ->times(2)
            ->with([
                'index' => 'table',
                'body' => [
                    'query' => [
                        'bool' => [
                            'must' => [
                                [
                                    'query_string' => [
                                        'query' => 'zonda',
                                    ],
                                ],
                            ],
                            'must_not' => [],
                        ],
                    ],
                    'stored_fields' => 'foo',
                    'aggregations' => [
                        'foo' => [
                            'terms' => [
                                'field' => 'foo.raw',
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
                    'sort' => [],
                ],
            ]);
        $openSearchEngine = new OpenSearchEngine($client);

        Builder::macro('distinct', function ($field) use ($openSearchEngine): \Illuminate\Support\Collection {
            $this->distinctField = $field;

            /** @phpstan-ignore-next-line */
            $results = $openSearchEngine->search($this);

            // @phpstan-ignore-next-line
            if (Arr::has($results, sprintf('aggregations.%s.buckets', $this->distinctField))) {
                // @phpstan-ignore-next-line
                return collect(Arr::get($results, sprintf('aggregations.%s.buckets', $this->distinctField)))->pluck(
                    'key'
                );
            }

            return collect();
        });
        $builder = new Builder(new SearchableModel(), 'zonda');
        $builder->distinct('foo')
            ->count();
        $openSearchEngine->search($builder);
    }

    public function testMapCorrectlyMapsResultsToModels(): void
    {
        $client = m::mock(Client::class);
        $openSearchEngine = new OpenSearchEngine($client);

        $model = m::mock(SearchableModel::class);
        $model->shouldReceive('getScoutModelsByIds')
            ->andReturn($models = Collection::make([
                new SearchableModel([
                    'id' => 1,
                ]),
            ]));

        $builder = m::mock(Builder::class);

        $results = $openSearchEngine->map($builder, [
            'nbHits' => 1,
            'hits' => [
                [
                    '_id' => 1,
                    'id' => 1,
                ],
            ],
        ], $model);

        $this->assertCount(1, $results);
    }

    public function testMapMethodRespectsOrder(): void
    {
        $client = m::mock(Client::class);
        $openSearchEngine = new OpenSearchEngine($client);

        $model = m::mock(SearchableModel::class);
        $model->shouldReceive('getScoutModelsByIds')
            ->andReturn($models = Collection::make([
                new SearchableModel([
                    'id' => 1,
                ]),
                new SearchableModel([
                    'id' => 2,
                ]),
                new SearchableModel([
                    'id' => 3,
                ]),
                new SearchableModel([
                    'id' => 4,
                ]),
            ]));

        $builder = m::mock(Builder::class);

        $results = $openSearchEngine->map($builder, [
            'nbHits' => 4,
            'hits' => [
                [
                    '_id' => 1,
                    'id' => 1,
                ],
                [
                    '_id' => 2,
                    'id' => 2,
                ],
                [
                    '_id' => 4,
                    'id' => 4,
                ],
                [
                    '_id' => 3,
                    'id' => 3,
                ],
            ],
        ], $model);

        $this->assertCount(4, $results);

        // It's important we assert with array keys to ensure
        // they have been reset after sorting.
        $this->assertSame([
            0 => [
                'id' => 1,
            ],
            1 => [
                'id' => 2,
            ],
            2 => [
                'id' => 4,
            ],
            3 => [
                'id' => 3,
            ],
        ], $results->toArray());
    }

    public function testLazyMapCorrectlyMapsResultsToModels(): void
    {
        if (! method_exists(Builder::class, 'cursor')) {
            $this->markTestSkipped('Support for cursor available since 9.0.');
        }

        $client = m::mock(Client::class);
        $openSearchEngine = new OpenSearchEngine($client);

        $model = m::mock(SearchableModel::class);
        $model->shouldReceive('queryScoutModelsByIds->cursor')
            ->andReturn($models = LazyCollection::make([
                new SearchableModel([
                    'id' => 1,
                ]),
            ]));

        $builder = m::mock(Builder::class);

        $results = $openSearchEngine->lazyMap($builder, [
            'nbHits' => 1,
            'hits' => [
                [
                    '_id' => 1,
                    'id' => 1,
                ],
            ],
        ], $model);

        $this->assertCount(1, $results);
    }

    public function testLazyMapMethodRespectsOrder(): void
    {
        if (! method_exists(Builder::class, 'cursor')) {
            $this->markTestSkipped('Support for cursor available since 9.0.');
        }

        $client = m::mock(Client::class);
        $openSearchEngine = new OpenSearchEngine($client);

        $model = m::mock(SearchableModel::class);
        $model->shouldReceive('queryScoutModelsByIds->cursor')
            ->andReturn($models = LazyCollection::make([
                new SearchableModel([
                    'id' => 1,
                ]),
                new SearchableModel([
                    'id' => 2,
                ]),
                new SearchableModel([
                    'id' => 3,
                ]),
                new SearchableModel([
                    'id' => 4,
                ]),
            ]));

        $builder = m::mock(Builder::class);

        $results = $openSearchEngine->lazyMap($builder, [
            'hits' => [
                [
                    '_id' => 1,
                    'id' => 1,
                ],
                [
                    '_id' => 2,
                    'id' => 2,
                ],
                [
                    '_id' => 4,
                    'id' => 4,
                ],
                [
                    '_id' => 3,
                    'id' => 3,
                ],
            ],
        ], $model);

        $this->assertCount(4, $results);

        // It's important we assert with array keys to ensure
        // they have been reset after sorting.
        $this->assertSame([
            0 => [
                'id' => 1,
            ],
            1 => [
                'id' => 2,
            ],
            2 => [
                'id' => 4,
            ],
            3 => [
                'id' => 3,
            ],
        ], $results->toArray());
    }

    public function testAModelIsIndexedWithACustomAlgoliaKey(): void
    {
        $client = m::mock(Client::class);
        $customKeySearchableModel = new CustomKeySearchableModel([
            'id' => 1,
        ]);
        $client->shouldReceive('bulk')
            ->once()
            ->with([
                'index' => 'table',
                'body' => [
                    [
                        'index' => [
                            '_index' => 'table',
                            '_id' => 'my-opensearch-key.1',
                        ],
                    ],
                    [
                        'id' => 1,
                        $customKeySearchableModel->getScoutKeyName() => $customKeySearchableModel->getScoutKey(),
                    ],
                ],
            ]);

        $openSearchEngine = new OpenSearchEngine($client);
        $openSearchEngine->update(Collection::make([$customKeySearchableModel]));
    }

    public function testAModelIsRemovedWithACustomAlgoliaKey(): void
    {
        $client = m::mock(Client::class);
        $client->shouldReceive('bulk')
            ->once()
            ->with([
                'index' => 'table',
                'body' => [
                    [
                        'delete' => [
                            '_index' => 'table',
                            '_id' => 'my-opensearch-key.1',
                        ],
                    ],
                ],
            ]);

        $openSearchEngine = new OpenSearchEngine($client);
        $openSearchEngine->delete(Collection::make([
            new CustomKeySearchableModel([
                'id' => 1,
            ]),
        ]));
    }

    public function testFlushAModelWithACustomAlgoliaKey(): void
    {
        $model = m::mock(CustomKeySearchableModel::class);
        $model->shouldReceive('searchableAs')
            ->once()
            ->withNoArgs()
            ->andReturn('table');
        $client = m::mock(Client::class);
        $client->shouldReceive('deleteByQuery')
            ->with([
                'index' => 'table',
                'body' => [
                    'query' => [
                        'match_all' => new \stdClass(),
                    ],
                ],
            ])
            ->andReturn('table');
        $openSearchEngine = new OpenSearchEngine($client);
        $openSearchEngine->flush($model);
    }

    public function testUpdateEmptySearchableArrayDoesNotAddObjectsToIndex(): void
    {
        $client = m::mock(Client::class);

        $client->shouldNotReceive('bulk');

        $openSearchEngine = new OpenSearchEngine($client);
        $openSearchEngine->update(Collection::make([new EmptySearchableModel()]));
    }

    public function testUpdateEmptySearchableArrayFromSoftDeletedModelDoesNotAddObjectsToIndex(): void
    {
        $client = m::mock(Client::class);
        $client->shouldNotReceive('bulk');

        $openSearchEngine = new OpenSearchEngine($client, true);
        $openSearchEngine->update(Collection::make([new SoftDeletedEmptySearchableModel()]));
    }

    public function testMapWithoutHits(): void
    {
        $client = m::mock(Client::class);
        $openSearchEngine = new OpenSearchEngine($client);

        $model = m::mock(SearchableModel::class);
        $model->shouldReceive('newCollection')
            ->andReturn($models = Collection::make());

        $builder = m::mock(Builder::class);

        $results = $openSearchEngine->map($builder, [
            'nbHits' => 1,
            'hits' => null,
        ], $model);

        $this->assertCount(0, $results);

        $results = $openSearchEngine->map($builder, null, $model);

        $this->assertCount(0, $results);
    }

    public function testLazyMapWithoutHits(): void
    {
        $client = m::mock(Client::class);
        $openSearchEngine = new OpenSearchEngine($client);

        $model = m::mock(SearchableModel::class);
        $model->shouldReceive('newCollection')
            ->andReturn($models = Collection::make());

        $builder = m::mock(Builder::class);

        $results = $openSearchEngine->lazyMap($builder, [
            'nbHits' => 1,
            'hits' => null,
        ], $model);

        $this->assertCount(0, $results);

        $results = $openSearchEngine->lazyMap($builder, null, $model);

        $this->assertCount(0, $results);
    }

    public function testOpenSearchClientMethod(): void
    {
        $client = m::mock(Client::class);
        $openSearchEngine = new OpenSearchEngine($client);
        $client->shouldReceive('nodes')
            ->withNoArgs()
            ->once();
        $openSearchEngine->nodes();
    }

    public function testMapIds(): void
    {
        $client = m::mock(Client::class);
        $openSearchEngine = new OpenSearchEngine($client);

        $results = $openSearchEngine->mapIds(null);

        $this->assertInstanceOf(\Illuminate\Support\Collection::class, $results);
    }
}
