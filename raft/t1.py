text_splitter = RecursiveCharacterTextSplitter(chunk_size=400, chunk_overlap=60)
result_simi = db.similarity_search(question, k=5)