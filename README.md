Queue-matching is project about queue matching in real time

I seted up 1v1 match with same exactly rating.

guide:
create new pool: NewPool1v1()
exported methods: 
- Join: player/client joins the pool
- Leave: player/client leaves the pool (if they are already in the pool)
- Match: get a buffer channel that matches players in pair (remove them from the pool)
- Visualize: get clone of the pool
