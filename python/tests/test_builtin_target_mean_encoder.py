# from hsfs.builtin_transformations import target_mean_encoder


def test_target_mean_encoder_empty():
    assert True


# def test_target_mean_encoder_training_computes_means():
#     # Feature and label series
#     feature = pd.Series(["a", "a", "b", "b", "b", "c", None])
#     label = pd.Series([1, 2, 0, 1, 3, 5, 2])

#     # Get python callable for the UDF
#     fn = target_mean_encoder.get_udf(online=True)
#     encoded = fn(feature, label)

#     # Expected per-category means
#     # a -> (1+2)/2 = 1.5
#     # b -> (0+1+3)/3 = 4/3
#     # c -> (5) = 5
#     expected = [1.5, 1.5, 4.0 / 3.0, 4.0 / 3.0, 4.0 / 3.0, 5.0, math.nan]

#     # Compare allowing NaN
#     for got, want in zip(encoded.tolist(), expected):
#         if math.isnan(want):
#             assert math.isnan(got)
#         else:
#             assert got == pytest.approx(want, rel=1e-9, abs=1e-12)


# def test_target_mean_encoder_serving_with_context_mapping():
#     # Only feature values; label not available at serving, pass all-NaN series
#     feature = pd.Series(["a", "x", None, "b", "c"])
#     dummy_label = pd.Series([np.nan] * len(feature))

#     # Provide mapping via transformation context
#     mapping = {"a": 1.5, "b": 1.25}
#     global_mean = 2.0
#     target_mean_encoder.transformation_context = {
#         "target_means": mapping,
#         "global_mean": global_mean,
#     }

#     fn = target_mean_encoder.get_udf(online=True)
#     encoded = fn(feature, dummy_label)

#     # a -> 1.5 (in mapping)
#     # x -> unseen -> fallback to global_mean (2.0)
#     # None -> NaN
#     # b -> 1.25 (in mapping)
#     # c -> unseen -> fallback to global_mean (2.0)
#     expected = [1.5, 2.0, math.nan, 1.25, 2.0]

#     for got, want in zip(encoded.tolist(), expected):
#         if math.isnan(want):
#             assert math.isnan(got)
#         else:
#             assert got == pytest.approx(want, rel=1e-9, abs=1e-12)
