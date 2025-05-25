#!/usr/bin/env python3
"""
Sample data loader for the distributed search engine.
This script loads sample documents to test the system functionality.
"""

import requests
import json
import time
import sys
from typing import List, Dict

# API Gateway URL
API_BASE_URL = "http://localhost:8080"

# Sample documents for testing
SAMPLE_DOCUMENTS = [
    {
        "title": "Introduction to Machine Learning",
        "content": """Machine learning is a subset of artificial intelligence (AI) that provides systems the ability to automatically learn and improve from experience without being explicitly programmed. Machine learning focuses on the development of computer programs that can access data and use it to learn for themselves. The process of learning begins with observations or data, such as examples, direct experience, or instruction, in order to look for patterns in data and make better decisions in the future based on the examples that we provide.""",
        "source": "ml-tutorial",
        "metadata": {"category": "education", "difficulty": "beginner", "tags": ["ai", "ml", "tutorial"]}
    },
    {
        "title": "Deep Learning Fundamentals",
        "content": """Deep learning is part of a broader family of machine learning methods based on artificial neural networks with representation learning. Learning can be supervised, semi-supervised or unsupervised. Deep learning architectures such as deep neural networks, deep belief networks, recurrent neural networks and convolutional neural networks have been applied to fields including computer vision, speech recognition, natural language processing, machine translation, bioinformatics and drug design.""",
        "source": "dl-guide",
        "metadata": {"category": "education", "difficulty": "intermediate", "tags": ["deep learning", "neural networks", "ai"]}
    },
    {
        "title": "Natural Language Processing Overview",
        "content": """Natural language processing (NLP) is a subfield of linguistics, computer science, and artificial intelligence concerned with the interactions between computers and human language, in particular how to program computers to process and analyze large amounts of natural language data. The goal is a computer capable of understanding the contents of documents, including the contextual nuances of the language within them.""",
        "source": "nlp-intro",
        "metadata": {"category": "education", "difficulty": "intermediate", "tags": ["nlp", "linguistics", "ai"]}
    },
    {
        "title": "Computer Vision Applications",
        "content": """Computer vision is an interdisciplinary scientific field that deals with how computers can gain high-level understanding from digital images or videos. From the perspective of engineering, it seeks to understand and automate tasks that the human visual system can do. Computer vision tasks include methods for acquiring, processing, analyzing and understanding digital images, and extraction of high-dimensional data from the real world in order to produce numerical or symbolic information.""",
        "source": "cv-guide",
        "metadata": {"category": "education", "difficulty": "advanced", "tags": ["computer vision", "image processing", "ai"]}
    },
    {
        "title": "Reinforcement Learning Basics",
        "content": """Reinforcement learning (RL) is an area of machine learning concerned with how intelligent agents ought to take actions in an environment in order to maximize the notion of cumulative reward. Reinforcement learning is one of three basic machine learning paradigms, alongside supervised learning and unsupervised learning. Unlike supervised learning, reinforcement learning does not require labelled input/output pairs to be presented, and need not explicitly correct sub-optimal actions.""",
        "source": "rl-tutorial",
        "metadata": {"category": "education", "difficulty": "advanced", "tags": ["reinforcement learning", "agents", "ml"]}
    },
    {
        "title": "Data Science Methodology",
        "content": """Data science is an inter-disciplinary field that uses scientific methods, processes, algorithms and systems to extract knowledge and insights from many structural and unstructured data. Data science is related to data mining, machine learning and big data. Data science is a concept to unify statistics, data analysis, informatics, and their related methods in order to understand and analyze actual phenomena with data.""",
        "source": "ds-methodology",
        "metadata": {"category": "education", "difficulty": "beginner", "tags": ["data science", "statistics", "analysis"]}
    },
    {
        "title": "Big Data Analytics",
        "content": """Big data analytics is the use of advanced analytic techniques against very large, diverse data sets that include structured, semi-structured and unstructured data, from different sources, and in different sizes from terabytes to zettabytes. Big data analytics allows analysts, researchers and business users to make better and faster decisions using data that was previously inaccessible or unusable.""",
        "source": "big-data-guide",
        "metadata": {"category": "technology", "difficulty": "intermediate", "tags": ["big data", "analytics", "data processing"]}
    },
    {
        "title": "Cloud Computing Fundamentals",
        "content": """Cloud computing is the on-demand availability of computer system resources, especially data storage and computing power, without direct active management by the user. The term is generally used to describe data centers available to many users over the Internet. Large clouds, predominant today, often have functions distributed over multiple locations from central servers.""",
        "source": "cloud-intro",
        "metadata": {"category": "technology", "difficulty": "beginner", "tags": ["cloud", "computing", "infrastructure"]}
    },
    {
        "title": "Cybersecurity Best Practices",
        "content": """Cybersecurity is the practice of protecting systems, networks, and programs from digital attacks. These cyberattacks are usually aimed at accessing, changing, or destroying sensitive information; extorting money from users; or interrupting normal business processes. Implementing effective cybersecurity measures is particularly challenging today because there are more devices than people, and attackers are becoming more innovative.""",
        "source": "security-guide",
        "metadata": {"category": "technology", "difficulty": "intermediate", "tags": ["security", "cybersecurity", "protection"]}
    },
    {
        "title": "Blockchain Technology Explained",
        "content": """A blockchain is a growing list of records, called blocks, that are linked and secured using cryptography. Each block contains a cryptographic hash of the previous block, a timestamp, and transaction data. By design, a blockchain is resistant to modification of its data. This is because once recorded, the data in any given block cannot be altered retroactively without alteration of all subsequent blocks.""",
        "source": "blockchain-intro",
        "metadata": {"category": "technology", "difficulty": "advanced", "tags": ["blockchain", "cryptocurrency", "distributed systems"]}
    },
    {
        "title": "Internet of Things (IoT) Overview",
        "content": """The Internet of things (IoT) describes the network of physical objects—things—that are embedded with sensors, software, and other technologies for the purpose of connecting and exchanging data with other devices and systems over the internet. These devices range from ordinary household objects to sophisticated industrial tools.""",
        "source": "iot-guide",
        "metadata": {"category": "technology", "difficulty": "beginner", "tags": ["iot", "sensors", "connectivity"]}
    },
    {
        "title": "Quantum Computing Introduction",
        "content": """Quantum computing is the use of quantum phenomena such as superposition and entanglement to perform computation. Computers that perform quantum computations are known as quantum computers. Quantum computers are believed to be able to solve certain computational problems, such as integer factorization, substantially faster than classical computers.""",
        "source": "quantum-intro",
        "metadata": {"category": "technology", "difficulty": "advanced", "tags": ["quantum", "computing", "physics"]}
    },
    {
        "title": "Software Engineering Principles",
        "content": """Software engineering is the systematic application of engineering approaches to the development of software. Software engineering is a computing discipline. A software engineer is a person who applies the principles of software engineering to design, develop, maintain, test, and evaluate computer software.""",
        "source": "se-principles",
        "metadata": {"category": "programming", "difficulty": "intermediate", "tags": ["software engineering", "development", "principles"]}
    },
    {
        "title": "Web Development Fundamentals",
        "content": """Web development is the work involved in developing a website for the Internet or an intranet. Web development can range from developing a simple single static page of plain text to complex web applications, electronic businesses, and social network services. A more comprehensive list of tasks to which web development commonly refers, may include web engineering, web design, web content development, client liaison, client-side/server-side scripting, web server and network security configuration, and e-commerce development.""",
        "source": "web-dev-guide",
        "metadata": {"category": "programming", "difficulty": "beginner", "tags": ["web development", "html", "css", "javascript"]}
    },
    {
        "title": "Mobile App Development",
        "content": """Mobile app development is the act or process by which a mobile app is developed for mobile devices, such as personal digital assistants, enterprise digital assistants or mobile phones. These software applications are designed to run on mobile devices, such as a smartphone or tablet computer.""",
        "source": "mobile-dev-guide",
        "metadata": {"category": "programming", "difficulty": "intermediate", "tags": ["mobile", "app development", "ios", "android"]}
    }
]

def check_service_health() -> bool:
    """Check if the API Gateway is healthy."""
    try:
        response = requests.get(f"{API_BASE_URL}/health", timeout=5)
        return response.status_code == 200
    except requests.RequestException:
        return False

def index_document(document: Dict) -> Dict:
    """Index a single document."""
    try:
        response = requests.post(
            f"{API_BASE_URL}/index",
            json=document,
            timeout=30
        )
        return {
            "success": response.status_code == 200,
            "response": response.json() if response.status_code == 200 else response.text,
            "status_code": response.status_code
        }
    except requests.RequestException as e:
        return {
            "success": False,
            "response": str(e),
            "status_code": None
        }

def bulk_index_documents(documents: List[Dict]) -> Dict:
    """Index multiple documents in bulk."""
    try:
        response = requests.post(
            f"{API_BASE_URL}/bulk-index",
            json={"documents": documents},
            timeout=60
        )
        return {
            "success": response.status_code == 200,
            "response": response.json() if response.status_code == 200 else response.text,
            "status_code": response.status_code
        }
    except requests.RequestException as e:
        return {
            "success": False,
            "response": str(e),
            "status_code": None
        }

def test_search(query: str) -> Dict:
    """Test search functionality."""
    try:
        response = requests.post(
            f"{API_BASE_URL}/search",
            json={"query": query, "size": 5},
            timeout=10
        )
        return {
            "success": response.status_code == 200,
            "response": response.json() if response.status_code == 200 else response.text,
            "status_code": response.status_code
        }
    except requests.RequestException as e:
        return {
            "success": False,
            "response": str(e),
            "status_code": None
        }

def test_typeahead(prefix: str) -> Dict:
    """Test typeahead functionality."""
    try:
        response = requests.get(
            f"{API_BASE_URL}/typeahead",
            params={"q": prefix, "limit": 5},
            timeout=10
        )
        return {
            "success": response.status_code == 200,
            "response": response.json() if response.status_code == 200 else response.text,
            "status_code": response.status_code
        }
    except requests.RequestException as e:
        return {
            "success": False,
            "response": str(e),
            "status_code": None
        }

def get_system_stats() -> Dict:
    """Get system statistics."""
    try:
        response = requests.get(f"{API_BASE_URL}/stats", timeout=10)
        return {
            "success": response.status_code == 200,
            "response": response.json() if response.status_code == 200 else response.text,
            "status_code": response.status_code
        }
    except requests.RequestException as e:
        return {
            "success": False,
            "response": str(e),
            "status_code": None
        }

def main():
    """Main function to load sample data and test the system."""
    print("🚀 Starting sample data loading for Distributed Search Engine")
    print("=" * 60)
    
    # Check service health
    print("1. Checking service health...")
    if not check_service_health():
        print("❌ API Gateway is not healthy. Please ensure the system is running.")
        print("   Run: docker-compose up -d")
        sys.exit(1)
    print("✅ API Gateway is healthy")
    
    # Load sample data
    print("\n2. Loading sample documents...")
    
    # Option 1: Bulk index (faster)
    print("   Using bulk indexing for better performance...")
    result = bulk_index_documents(SAMPLE_DOCUMENTS)
    
    if result["success"]:
        response = result["response"]
        print(f"✅ Bulk indexing completed:")
        print(f"   - Total documents: {response.get('total_documents', 0)}")
        print(f"   - Successful: {response.get('successful', 0)}")
        print(f"   - Failed: {response.get('failed', 0)}")
        print(f"   - Processing time: {response.get('processing_time', 0):.2f}s")
    else:
        print(f"❌ Bulk indexing failed: {result['response']}")
        
        # Fallback to individual indexing
        print("   Falling back to individual document indexing...")
        successful = 0
        failed = 0
        
        for i, doc in enumerate(SAMPLE_DOCUMENTS, 1):
            print(f"   Indexing document {i}/{len(SAMPLE_DOCUMENTS)}: {doc['title'][:50]}...")
            result = index_document(doc)
            
            if result["success"]:
                successful += 1
                print(f"   ✅ Indexed: {result['response'].get('document_id', 'unknown')}")
            else:
                failed += 1
                print(f"   ❌ Failed: {result['response']}")
            
            time.sleep(0.5)  # Small delay to avoid overwhelming the system
        
        print(f"\n   Individual indexing completed: {successful} successful, {failed} failed")
    
    # Wait for indexing to complete
    print("\n3. Waiting for indexing to complete...")
    time.sleep(3)
    
    # Test search functionality
    print("\n4. Testing search functionality...")
    test_queries = [
        "machine learning",
        "neural networks",
        "data science",
        "cloud computing",
        "cybersecurity"
    ]
    
    for query in test_queries:
        print(f"   Searching for: '{query}'")
        result = test_search(query)
        
        if result["success"]:
            response = result["response"]
            hits = response.get("hits", [])
            print(f"   ✅ Found {len(hits)} results (total: {response.get('total', 0)})")
            
            for hit in hits[:2]:  # Show first 2 results
                print(f"      - {hit.get('title', 'No title')} (score: {hit.get('score', 0):.3f})")
        else:
            print(f"   ❌ Search failed: {result['response']}")
        
        time.sleep(0.5)
    
    # Test typeahead functionality
    print("\n5. Testing typeahead functionality...")
    test_prefixes = ["mach", "data", "comp", "cyber", "block"]
    
    for prefix in test_prefixes:
        print(f"   Typeahead for: '{prefix}'")
        result = test_typeahead(prefix)
        
        if result["success"]:
            response = result["response"]
            suggestions = response.get("suggestions", [])
            print(f"   ✅ Found {len(suggestions)} suggestions")
            
            for suggestion in suggestions[:3]:  # Show first 3 suggestions
                print(f"      - {suggestion.get('term', 'No term')} (freq: {suggestion.get('frequency', 0)})")
        else:
            print(f"   ❌ Typeahead failed: {result['response']}")
        
        time.sleep(0.5)
    
    # Get system statistics
    print("\n6. Getting system statistics...")
    result = get_system_stats()
    
    if result["success"]:
        stats = result["response"]
        print("✅ System statistics:")
        print(f"   - Search services: {stats.get('healthy_search_services', 0)}/{stats.get('search_services', 0)}")
        print(f"   - Typeahead services: {stats.get('healthy_typeahead_services', 0)}/{stats.get('typeahead_services', 0)}")
        print(f"   - Indexing services: {stats.get('healthy_indexing_services', 0)}/{stats.get('indexing_services', 0)}")
        print(f"   - Redis connected: {stats.get('cache_info', {}).get('redis_connected', False)}")
    else:
        print(f"❌ Failed to get statistics: {result['response']}")
    
    print("\n" + "=" * 60)
    print("🎉 Sample data loading and testing completed!")
    print("\nYou can now:")
    print("- Search documents: curl -X POST http://localhost:8080/search -H 'Content-Type: application/json' -d '{\"query\": \"machine learning\", \"size\": 10}'")
    print("- Get suggestions: curl 'http://localhost:8080/typeahead?q=mach&limit=5'")
    print("- View statistics: curl http://localhost:8080/stats")

if __name__ == "__main__":
    main() 