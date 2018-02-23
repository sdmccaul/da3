var res = db.pvd.aggregate({$group : { _id : {"year": "$created.year", "month": "$created.month" }, count: {$sum :1 } } });
printjson(res.toArray());