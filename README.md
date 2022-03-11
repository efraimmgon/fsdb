Implementation of a basic filesystem database, based on the source code for hacker news when it was run by pg (https://github.com/wting/hackernews), which is supposed to be the same thing they used at Viaweb when it was sold to Yahoo! (http://www.paulgraham.com/vwfaq.html):

```
What database did you use?

We didn't use one. We just stored everything in files. The Unix file system is pretty good at not losing your data, especially if you put the files on a Netapp.

It is a common mistake to think of Web-based apps as interfaces to databases. Desktop apps aren't just interfaces to databases; why should Web-based apps be any different? The hard part is not where you store the data, but what the software does.

While we were doing Viaweb, we took a good deal of heat from pseudo-technical people like VCs and industry analysts for not using a database-- and for using cheap Intel boxes running FreeBSD as servers. But when we were getting bought by Yahoo, we found that they also just stored everything in files-- and all their servers were also cheap Intel boxes running FreeBSD.
```

Anyway, based on comments on hackernews' performance, it seems to work well as long as your server can hold the data you need in memory. In other words, it does not scale very well, but for prototyping and for simple/small programs it seems to be worth it, since you don't have to worry about all the usual database configurations that may take some time to figure out in the beginning of most projects.