// For n <= 0, the sum is empty and returns 0.

/**
 * Iterative approach: O(n) time and O(1) additional space.
 */
export function sum_to_n_a(n: number): number {
	if (n <= 0) return 0;

	let sum = 0;
	for (let value = 1; value <= n; value++) {
		sum += value;
	}
	return sum;
}

/**
 * Recursive approach: O(n) time and O(n) call-stack space.
 * Use the iterative or arithmetic-series version for large n to avoid stack limits.
 */
export function sum_to_n_b(n: number): number {
	if (n <= 0) return 0;
	return n + sum_to_n_b(n - 1);
}

/**
 * Arithmetic-series approach: O(1) time and O(1) additional space.
 * Dividing an even factor first keeps the intermediate product no larger than needed.
 */
export function sum_to_n_c(n: number): number {
	if (n <= 0) return 0;

	return n % 2 === 0
		? (n / 2) * (n + 1)
		: n * ((n + 1) / 2);
}

function runExamples(): void {
	const examples = [
		{ n: -5, expected: 0 },
		{ n: 0, expected: 0 },
		{ n: 1, expected: 1 },
		{ n: 2, expected: 3 },
		{ n: 3, expected: 6 },
		{ n: 5, expected: 15 },
		{ n: 10, expected: 55 },
		{ n: 25, expected: 325 },
		{ n: 100, expected: 5_050 },
		{ n: 500, expected: 125_250 },
	];
	const results = examples.map(({ n, expected }) => {
		const iterative = sum_to_n_a(n);
		const recursive = sum_to_n_b(n);
		const arithmetic = sum_to_n_c(n);

		if (
			iterative !== expected ||
			recursive !== expected ||
			arithmetic !== expected
		) {
			throw new Error(`Unexpected sum for n=${n}; expected ${expected}`);
		}

		return { n, expected, iterative, recursive, arithmetic };
	});

	console.table(results);

	const largeN = 100_000_000;
	const largeExpected = 5_000_000_050_000_000;
	if (sum_to_n_c(largeN) !== largeExpected) {
		throw new Error(`Unexpected arithmetic sum for n=${largeN}`);
	}
	console.log(
		`Arithmetic approach for n=${largeN}: ${largeExpected}`,
	);
}

runExamples();
