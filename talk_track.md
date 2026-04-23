# Priority & Fairness — 90-Second Talk Track

## Hook

Most AI products underserve their best users. I'll show you how this can happen, and how to fix it.

## Set the stage

This is a simulation of an AI assistant running on Temporal. Each colored block is a chat turn — a user's request moving through the system. They flow from Queued, to Running, to Completed. Free, Plus, and Pro users all share the same system.

## The priority problem

Here, I'm showing what happens by default - requests are processed on a first-come, first-serve basis. You can see here that our high-paying Pro users are waiting in line behind a bunch of free users. Now let's imagine Free usage spikes in a viral moment. And here are your $200-a-month Pro customers, queuing up at the back. They wait. The customer who pays the most is waiting the longest.

## Priority, the fix

Now when I turn on Priority, Pro runs before Plus, which runs before Free. Your paying customers jump to the front. Free users still run — just when capacity opens up.

## The fairness problem and fix

Now say within your Pro tier, you have different sized customers — BigCorp, MidCo, and Startup. Watch what happens. When BigCorp spikes, it totally dominates the capacity, starving out MidCo and Startup. With fairness, you can ensure your small customers don't get throttled by load from your biggest ones. 

And if you still want BigCorp to get more capacity — just not all of it — turn on weighted fairness. Here, I've set fairness weights such that BigCorp can execute up to about seventy percent of the time, but no more. MidCo and Startup still get served.

## Recap

So that's priority and fairness. Priority makes sure paying customers are served first. Fairness keeps a single heavy customer from blocking everyone else. And weights let you tune the mix to match how your business actually works.
