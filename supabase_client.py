from supabase import create_client
import os
from dotenv import load_dotenv

load_dotenv()

class SupabaseClient:
    def __init__(self):
        self.url = os.getenv("SUPABASE_URL")
        self.key = os.getenv("SUPABASE_KEY")
        self.client = create_client(self.url, self.key)
    
    def get_user_submissions(self, user_id):
        """Get submissions for a user"""
        response = self.client.table('submissions')\
            .select('*, questions(title)')\
            .eq('user_id', user_id)\
            .order('timestamp', desc=True)\
            .execute()
        return response.data
    
    def get_question_stats(self, question_id):
        """Get statistics for a question"""
        response = self.client.table('submissions')\
            .select('verdict', count='exact')\
            .eq('question_id', question_id)\
            .execute()
        return response.data
    
    # Add more helper methods as needed

supabase = SupabaseClient()
