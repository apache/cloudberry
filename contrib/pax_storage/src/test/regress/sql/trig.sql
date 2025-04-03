-- Test cosh(), sinh() and tanh() against SQL implementations.
-- Using a full precision test here is going to throw up small differences
-- in precision around the implementation in the backend. So, limit to 8
-- significant figures.
-- If all is working as expected, we should return no rows.
select cosh, cosh2, sinh, sinh2, tanh, tanh2 from 
	(select cosh::numeric(20, 8), cosh2::numeric(20, 8),
			sinh::numeric(20, 8), sinh2::numeric(20, 8),
			tanh::numeric(20, 8), (sinh2/cosh2)::numeric(20, 8) as tanh2 from 
		(select i, cosh(i), sinh(i), tanh(i),
				((exp(i)::float8 + exp(-i)::float8)/2)::float8 as cosh2,
				((exp(i)::float8 - exp(-i)::float8)/2)::float8 as sinh2 from
			(select i/1000. as i from generate_series(-10000, 10000, 20) i) j) k) l
	where cosh <> cosh2 or sinh <> sinh2 or tanh <> tanh2;
