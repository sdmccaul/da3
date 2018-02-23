var res = db.pvd.aggregate([ {'$group' : { '_id': '$created.user', 'count' : { '$sum' : 1} } }, {$sort : {count : -1 } } ]);
printjson(res.toArray());