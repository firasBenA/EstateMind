import { createClient } from "@supabase/supabase-js"

const supabaseUrl = "https://amxnojlfczwffvtwutrb.supabase.co"
const supabaseAnonKey = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImFteG5vamxmY3p3ZmZ2dHd1dHJiIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzU3MjE3NDMsImV4cCI6MjA5MTI5Nzc0M30.hxj1C-NiJ2DSWK1p_63OgYtwX2uzjSLS1osMuek9Ow0"

export const supabase = createClient(supabaseUrl, supabaseAnonKey)