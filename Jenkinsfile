pipeline {
    agent any

    environment {
        // Define environment variables if needed
        VENV_PATH = 'milestone2'
    }
    

    stages {
        stage('Setup Virtual Environment and Install Dependencies') {
            steps {
                echo 'Setting up a shared virtual environment and installing packages...'
                sh '''#!/bin/bash
                sudo apt install python3.8-venv -y
                python3 -m venv $VENV_PATH
                source $VENV_PATH/bin/activate
                sudo pip install --upgrade pip
                '''
                // Assuming a shared requirements.txt at the root for common dependencies
                // If each directory has its own requirements.txt, move this into each stage
                sh 'sudo pip install -r requirements.txt'
            }
        }

        stage('Test Fetch') {
            steps {
                echo 'Testing Fetch...'
                dir('Milestone2/FINISHED_S01_fetch_training_data') {
                    // Replace with the actual command to run your script
                    sh 'coverage run -m pytest test_api.py'
                    sh 'coverage report -m'
                }
            }
        }

        stage('Test SVD Model') {
            steps {
                echo 'Testing SVD model...'
                dir('Milestone2/FINISHED_S02_train_SVD') {
                    // Replace with the actual command to run your script
                    sh 'coverage run -m pytest test_train.py'
                    sh 'coverage report -m'
                    sh 'coverage run -m pytest test_prepare.py'
                    sh 'coverage report -m'
                }
            }
        }

        stage('Test offline evaluation') {
            steps {
                echo 'Testing offline evaluation...'
                dir('Milestone2/FINISHED_S05_offline_eval') {
                    // Replace with the actual command to run your script
                    sh 'coverage run -m pytest test_offline_eval.py'
                    sh 'coverage report -m'
                }
            }
        }
    }

    post {
        always {
            echo 'Cleaning up...'
            sh 'rm -rf $VENV_PATH'
            // Additional cleanup actions can be performed here
        }
    }
    
}