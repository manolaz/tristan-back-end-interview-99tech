# Problem 4: Three ways to sum to n

`sum.ts` provides three implementations of the sum `1 + 2 + ... + n`:

| Function | Approach | Time | Additional space |
| --- | --- | --- | --- |
| `sum_to_n_a` | Iterative loop | O(n) | O(1) |
| `sum_to_n_b` | Recursion | O(n) | O(n) call stack |
| `sum_to_n_c` | Arithmetic-series formula | O(1) | O(1) |

All functions return `0` for `n <= 0` (an empty sum). The recursive version is
intended to demonstrate a different approach; use the formula for large inputs
to avoid recursion depth limits.

The examples in `sum.ts` print a comparison table, check expected results, and
show the arithmetic formula on a large input. With Node.js 22 or newer, run:

```sh
node --experimental-strip-types problem4/sum.ts
```
