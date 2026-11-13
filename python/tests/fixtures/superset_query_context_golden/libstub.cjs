// Stands in for modules the query code path never calls (react, xss, nanoid, seedrandom, translation).
module.exports = { t: s => s, tn: s => s, nanoid: () => 'x', getDefaultWhiteList: () => ({}), FilterXSS: function () {} };
