from backend.database.supabase import supabase

response = (
    supabase.table("cases")
    .select("*")
    .limit(1)
    .execute()
)

print(response.data)