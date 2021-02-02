


```
http://localhost:3000/units-for-slot/QmQ696XttGrC456X3uBDMViPAJi24VJmbXAzayNw5maoLe
```

Units-for-slot:

* Local:

```
wrk2 -t 4 -c1000 -d30s -R100000000 --latency http://localhost:3000/units-for-slot/QmQ696XttGrC456X3uBDMViPAJi24VJmbXAzayNw5maoLe
```

* Production:

```
wrk2 -t 4 -c1000 -d30s -R100000000 --latency https://supermarket.adex.network/units-for-slot/QmQ696XttGrC456X3uBDMViPAJi24VJmbXAzayNw5maoLe
```

* Market:

```
wrk2 -t4 -c1000 -d30s -R100000000 --latency https://market.adex.network/units-for-slot/QmQ696XttGrC456X3uBDMViPAJi24VJmbXAzayNw5maoLe
```

Single Slots (proxy):

* Local:

```
wrk2 -t 4 -c1000 -d30s -R100000000 --latency http://localhost:3000/slots/QmQ696XttGrC456X3uBDMViPAJi24VJmbXAzayNw5maoLe
```

* Production:

```
wrk2 -t 4 -c1000 -d30s -R100000000 --latency https://supermarket.adex.network/slots/QmQ696XttGrC456X3uBDMViPAJi24VJmbXAzayNw5maoLe
```

* Market:

```
wrk2 -t4 -c1000 -d30s -R100000000 --latency https://market.adex.network/slots/QmQ696XttGrC456X3uBDMViPAJi24VJmbXAzayNw5maoLe
```
