const result = require("./result.json");

const cityTotalCount = result.facets.city.reduce(
  (sum, item) => sum + item.count,
  0,
);

const countyTotalCount = result.facets.county.reduce(
  (sum, item) => sum + item.count,
  0,
);

const regionTotalCount = result.facets.region.reduce(
  (sum, item) => sum + item.count,
  0,
);

console.log(cityTotalCount, countyTotalCount, regionTotalCount, cityTotalCount + countyTotalCount + regionTotalCount);
