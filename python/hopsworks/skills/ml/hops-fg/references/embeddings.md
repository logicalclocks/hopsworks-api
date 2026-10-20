# Vector embeddings

A feature group with an `embedding_index` is backed by the **vector database**
(OpenSearch), not RonDB. Vector DBs are optimized for read-heavy similarity
search, not frequent updates, and every update to ANY feature in the FG triggers
a vector DB write (re-indexing). So keep embedding FGs **minimal**: the primary
key, the vector, and only static or rarely-changing metadata. Put
frequently-updated features (real-time counters, scores, `last_login`) in a
**separate** RonDB-backed online FG with the same primary key, and join them in
the feature view.

```python
from hsfs.embedding import EmbeddingIndex, EmbeddingFeature, SimilarityFunctionType
from hsfs.feature import Feature

embedding_index = EmbeddingIndex()
embedding_index.add_embedding(
    name="text_embedding",
    dimension=384,
    similarity_function_type=SimilarityFunctionType.COSINE,  # or L2, DOT_PRODUCT
)

fg = fs.get_or_create_feature_group(
    name="document_embeddings",
    version=1,
    embedding_index=embedding_index,
    primary_key=["doc_id"],
    features=[
        Feature("doc_id", "bigint"),
        Feature("text_embedding", "array<float>"),
        # as few other features as possible, and only ones that rarely change
    ],
    online_enabled=True,
    stream=True,
)
```

## Similarity search

For FGs with an `embedding_index`, find nearest neighbors with optional filters:

```python
# Basic nearest neighbor search
results = fg.find_neighbors([0.1, 0.2, 0.3], k=5)

# With filters applied to the search
results = fg.find_neighbors(
    [0.1, 0.2, 0.3],
    k=5,
    filter=(fg.id1 > 10) & (fg.id1 < 30),
)
```
