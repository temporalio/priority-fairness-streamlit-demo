# Priority & Fairness — Talk Track

I'm John from the Temporal Product team, and I'm going to run through a quick demo of Priority and Fairness.

Say we're operating an AI chat application with Free, Plus, and Pro users, and each chat turn is running a Temporal workflow. You can see that each turn is executing on a first-come, first-serve basis, regardless of the customer's tier.

When there is a large spike in free traffic, the pro traffic is gonna end up waiting behind those free users, which is undesirable.

To avoid this issue, we can use Priority, which enforces that high priority workloads execute ahead of lower-priority workloads. You can set Priority on a Workflow or Activity, and in the temporal UI, and we can see the priority setting here.

So now if we have a big spike in free traffic, our pro users, when they come in, are gonna jump the line. This enables us to provide a higher level of service to our paying customers, while still serving the free tier.

Now say within the Pro tier, we have Startups, MidCos, and BigCorps, the latter of which can have large traffic surges. You can see that the spike coming from the BigCorps is interfering with the Startup and MidCo executions. 

In order to alleviate this noisy neighbor problem, we can turn on Fairness. With fairness, our executions are going to be interspersed according to the fairness weights, such that small and medium customers still have some room to execute, despite there being a big spike from the largest customer.

So, that's a quick demo of priority and fairness. Priority allows you to prioritize between different customer tiers, and fairness acts within a tier to ensure that each customer is treated fairly.
