var filter_limit = 1000;
var users = db.pvd.aggregate([ {$group : { _id : "$created.user", count: {$sum :1 } } }, { $match : { "count" : { $gte : filter_limit } } } ]).map( function(obj) { return obj._id });
var res = db.pvd.aggregate( [ {$match : { "created.user" : { $in: users } } }, {$group : { _id : {"year": "$created.year", "month": "$created.month", "user":"$created.user" }, count: {$sum :1 } } } ]);
printjson(res._batch);