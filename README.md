### Products
```
{
	"id": "foo",
	"parentIds": ["root","root2"]
}

{
	"id": "bar",
	"parentIds": ["root","root2"]
}

{
	"id": "boo",
	"parentIds": ["n1","root2"]
}
```

### Hierarchies
```
{
	"id": "n1",
	"parentId": "root"
}

{
	"id": "root",
	"parentId": null
}

{
	"id": "n2",
	"parentId": "root"
}

{
	"id": "root2",
	"parentId": null
}

{
	"id": "n3",
	"parentId": "root3"
}

{
	"id": "root3",
	"parentId": null
}

```

### Kafka
```
./kafka-console-consumer \
     --bootstrap-server localhost:9092 \
     --topic product-hierarchies 
```